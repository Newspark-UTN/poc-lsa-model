package org.edu.utn.newspark.lsa

import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.edu.utn.newspark.lemmatizer.News
import org.edu.utn.newspark.provider.MongoNewsProvider

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

object LSA extends MongoNewsProvider with App {
  // We need the number of concepts to make the SVD, else set it to default.
  val K = if (args.length > 0) args(0).toInt else 100
  val topConcepts = if (args.length > 1) args(1).toInt else 10
  val topTerms = if (args.length > 2) args(2).toInt else 5
  val topDocuments = if (args.length > 3) args(3).toInt else 3

  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("LSA Newspark")
  val sc = new SparkContext(conf)

  val allNews: List[News] = retrieveNews
  val numDocs = allNews.length

  // Lift to a parallel RDD
  val withoutLemmaRDD: RDD[News] = sc.parallelize(allNews)

  // Clean all the documents
  val lemmatizedRDD: RDD[(String, Seq[String])] = withoutLemmaRDD collect { case News(title, content) => (title, plainTextToLemmas(content, stopwords)) }

  val docTermFrequencies = lemmatizedRDD.mapValues(terms => {
    val termFreqsInDoc = terms.foldLeft(new mutable.HashMap[String, Int]()) {
      (map, term) => map += term -> (map.getOrElse(term, 0) + 1)
    }
    termFreqsInDoc
  })

  // We will cache this in memory since we are going to use it 2 times:
  // to calculate idfs and term document matrix
  docTermFrequencies.cache()

  val docIds = docTermFrequencies.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()

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
  def mergeDocumentFrequencies(dfs: mutable.HashMap[String, Int], tfs: (String, mutable.HashMap[String, Int])): mutable.HashMap[String, Int] = {
    tfs._2.keySet.foreach { term =>
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
  val idfs: mutable.HashMap[Term, IDF] = documentFrequencies map {
    case (term, docCount) => (term, math.log(numDocs.toDouble / docCount))
  }

  val termIds: Map[Term, Index] = idfs.keys.zipWithIndex.toMap
  println("TOOOOOOOOOOOOOOOOOOOOOOOOOooooooooooooooooooooooooooOOOOOOOOOOOOOOOOOOOOOOM")
  println(numDocs)
  println(termIds.keys.size)

  // Broadcast this map in order to have it available through all the executors, together with idfs.
  val broadcastTermIds = sc.broadcast(termIds).value
  val broadcastIdfs = sc.broadcast(idfs).value

  // Create the term document matrix
  import org.apache.spark.mllib.linalg.Vectors

  val termDocMatrix = docTermFrequencies map { case (_, document) =>
    val documentTotalTerms = document.values.sum
    val document_TF_IDFS: Seq[(Index, TF_IDF)] = document.collect {
      case (term, count) if broadcastTermIds.contains(term) && broadcastIdfs.contains(term) =>
        // tuples of the form (index, idf-tf)
        (broadcastTermIds(term), broadcastIdfs(term) * document(term) / documentTotalTerms)
    }.toSeq
    Vectors.sparse(broadcastTermIds.size, document_TF_IDFS)
  }

  import org.apache.spark.mllib.linalg.distributed.RowMatrix

  termDocMatrix.cache()
  val mat = new RowMatrix(termDocMatrix)
  val svd = mat.computeSVD(K, computeU=true)


  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix],
                            numConcepts: Int, numTerms: Int, termIds: Map[Long, String])
  : Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map {
        case (score, id) => (termIds(id), score)
      }
    }
    topTerms
  }

  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix],
                            numConcepts: Int, numDocs: Int, docIds: scala.collection.Map[Long, String])
  : Seq[Seq[(String, Double)]] = {
    val u = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
      topDocs += docWeights.top(numDocs).map{
        case (score, id) => (docIds(id), score)
      }
    }
    topDocs
  }

  val topConceptTerms = topTermsInTopConcepts(svd, topConcepts, topTerms, termIds.map{ case (term, index) => (index.toLong, term)})
  val topConceptDocs = topDocsInTopConcepts(svd, topConcepts, topDocuments, docIds)
  for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
    println("Concept terms: " + terms.map(_._1).mkString(", "))
    println("Concept docs: " + docs.map(_._1).mkString(", "))
    println()
  }
}
