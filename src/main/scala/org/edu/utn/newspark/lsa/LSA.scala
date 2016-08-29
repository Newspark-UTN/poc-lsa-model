package org.edu.utn.newspark.lsa

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.edu.utn.newspark.lemmatizer.News
import org.edu.utn.newspark.provider.MongoNewsProvider

import scala.collection.mutable

object LSA extends MongoNewsProvider with App {
  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("LSA Newspark")
  val sc = new SparkContext(conf)

  val allNews: List[News] = retrieveNews
  val numDocs = allNews.length

  // Lift to a parallel RDD
  val withoutLemmaRDD: RDD[News] = sc.parallelize(allNews)

  // Clean all the documents
  val lemmatizedRDD: RDD[Seq[String]] = withoutLemmaRDD collect { case News(_, content) => plainTextToLemmas(content, stopwords) }

  // Create a new rdd with the wordcounts (term, how many times the term appears in the document) of all documents.
  val docTermFrequencies: RDD[mutable.HashMap[String, Int]] = lemmatizedRDD map {terms =>
    terms.foldLeft(new mutable.HashMap[String, Int]()){
      (actualMap, term) =>
        actualMap += term -> (actualMap.getOrElse(term, 0) + 1)
        actualMap
    }
  }

  println("PRINTING DOCTERM")
  docTermFrequencies.collect.foreach(println)

  // We will cache this in memory since we are going to use it 2 times:
  // to calculate idfs and term document matrix
  docTermFrequencies.cache()

  // This will be our accumulator for the document frequencies
  val zero = new mutable.HashMap[String, Int]()

  /**
   * Counts the number of times each word of the tfs appears on all the corpus.
   * This happens in parallel in each of our mappers.
   *
   * @param dfs the acum df map
   * @param tfs the termFrequencies of a single document of the rdd we cached before.
   * @return the partial dfs result in the mapper executing the merge.
   */
  def mergeDocumentFrequencies(dfs: mutable.HashMap[String, Int], tfs: mutable.HashMap[String, Int]): mutable.HashMap[String, Int] = {
    tfs.keySet.foreach { term =>
    dfs += term -> (dfs.getOrElse(term, 0) + 1)
  }
    dfs
  }

  /**
   * Combines both partial dfs from the mappers into 1, reducing the computation we did before.
   *
   * @param dfs1 a partial dfs from mapper 1
   * @param dfs2 another partial dfs from mapper 2
   * @return the resulting map from the combination of both dfs1 and dfs2
   */
  def combinePartialDocumentFrequencies(dfs1: mutable.HashMap[String, Int], dfs2: mutable.HashMap[String, Int]): mutable.HashMap[String, Int] = {
    dfs2.foreach { case (term, count) =>
      dfs1 += term -> (dfs1.getOrElse(term, 0) + count)
    }
    dfs1
  }

  // Next, create a document frequency rdd of the form (term, number of documents in which it appears)
  val documentFrequencies: mutable.HashMap[String, Int] = docTermFrequencies.aggregate(zero)(mergeDocumentFrequencies, combinePartialDocumentFrequencies)

  // Generate the inverse document frequencies
  val idfs: mutable.HashMap[Term, IDF] = documentFrequencies map { case (term, docCount) =>
    (term, math.log(numDocs.toDouble / docCount))
  }

  val termIds: Map[Term, Index] = idfs.keys.zipWithIndex.toMap

  // Broadcast this map in order to have it available through all the executors, together with idfs.
  val broadcastTermIds = sc.broadcast(termIds).value
  val broadcastIdfs = sc.broadcast(idfs).value

  // Create the term document matrix
  import org.apache.spark.mllib.linalg.Vectors

  val termDocMatrix = docTermFrequencies map { document =>
    val documentLength = document.values.sum
    val document_TF_IDFS: Seq[(Index, TF_IDF)] = document.collect {
      case (term, count) if broadcastTermIds.contains(term) =>
        // tuples of the form (index, idf-tf)
        (broadcastTermIds(term), broadcastIdfs(term) * document(term) / documentLength)
    }.toSeq
    Vectors.sparse(broadcastTermIds.size, document_TF_IDFS)
  }
}
