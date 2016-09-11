package org.edu.utn.newspark.lsa

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.edu.utn.newspark.lemmatizer.News
import org.edu.utn.newspark.provider.MongoNewsProvider

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LSA extends MongoNewsProvider with App {
  // We need the number of concepts to make the SVD, else set it to default.
  val K = if (args.length > 0) args(0).toInt else 100
  val topConcepts = if (args.length > 1) args(1).toInt else 30
  val topTerms = if (args.length > 2) args(2).toInt else 4
  val topDocuments = if (args.length > 3) args(3).toInt else 30

  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("LSA Newspark")
  val sc = new SparkContext(conf)

  val allNews: List[News] = retrieveNews

  // For each tag, calculate svd, termIds, and docIds
  val LSARuns: Seq[(SVD, String, Map[Term, Index], Map[Long, String], Map[Long, mutable.HashMap[String, Int]])] = allNews.groupBy(news => news.tag).map { case (tag, newsPerTag) =>

    val numDocs = newsPerTag.length

    // Lift to a parallel RDD
    val withoutLemmaRDD: RDD[News] = sc.parallelize(newsPerTag)

    // Clean all the documents
    val lemmatizedRDD: RDD[(String, Seq[String])] = withoutLemmaRDD collect { case News(title, _, content) => (title, plainTextToLemmas(content, stopwords)) }

    val docTermFrequencies = lemmatizedRDD.mapValues(terms => {
      val termFreqsInDoc = terms.foldLeft(new mutable.HashMap[String, Int]()) {
        (map, term) => map += term -> (map.getOrElse(term, 0) + 1)
      }
      termFreqsInDoc
    })

    // We will cache this in memory since we are going to use it 2 times:
    // to calculate idfs and term document matrix
    docTermFrequencies.cache()

    val docIds: Map[Long, String] = docTermFrequencies.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap().toMap
    val docCorpus: Map[Long, mutable.HashMap[String, Int]] = docTermFrequencies.map(_._2).zipWithUniqueId().map(_.swap).collectAsMap().toMap

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

    termDocMatrix.cache()
    val mat = new RowMatrix(termDocMatrix)
    val svd = mat.computeSVD(K, computeU = true)

    (svd, tag, termIds, docIds, docCorpus)
  }.toSeq

    def docsWhichContainsTerms(results: Seq[(Seq[(String, Double, Int)], Seq[(String, Double, Long)])],
                               documents:  Map[Long, mutable.HashMap[String, Int]])
    : Seq[(Seq[(String, Double, Int)], Seq[(String, Double, Long)])] = {
      results.map {
        case (terms, docs) =>
          val termsStrings = terms.map(_._1)

          val filterDocs = docs.filter {
            case (_, _, id) =>
              val frequencies = documents.getOrElse(id, new mutable.HashMap[String, Int]())
              terms.forall {
                case (term, _, _) => frequencies.contains(term)
              }
          }

          (terms, filterDocs)
      }
    }

    def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix],
                              numConcepts: Int, numTerms: Int, termIds: Map[Long, String])
    : Seq[Seq[(String, Double, Int)]] = {
      val v = svd.V
      val topTerms = new ArrayBuffer[Seq[(String, Double, Int)]]()
      val arr = v.toArray
      for (i <- 0 until numConcepts.min(v.numRows)) {
        val offs = i * v.numRows
        val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
        val sorted = termWeights.sortBy(-_._1)
        topTerms += sorted.take(numTerms).map {
          case (score, id) => (termIds(id), score, id)
        }
      }
      topTerms
    }

    def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix],
                             numConcepts: Int, numDocs: Int, docIds: scala.collection.Map[Long, String])
    : Seq[Seq[(String, Double, Long)]] = {
      val u = svd.U
      val topDocs = new ArrayBuffer[Seq[(String, Double, Long)]]()
      for (i <- 0 until numConcepts.min(u.numRows().toInt)) {
        val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
        topDocs += docWeights.top(numDocs).map {
          case (score, id) => (docIds(id), score, id)
        }
      }
      topDocs
    }

  LSARuns.map { case (svd, tag, termIds, docIds, fullDocs) =>
    val topConceptTerms = topTermsInTopConcepts(svd, topConcepts, topTerms, termIds.map { case (term, index) => (index.toLong, term) })
    val topConceptDocs = topDocsInTopConcepts(svd, topConcepts, topDocuments, docIds)
    val results = docsWhichContainsTerms(topConceptTerms.zip(topConceptDocs), fullDocs)

    val filteredResults = results.filter {
      case (_, docs) => docs.nonEmpty
    }.groupBy {
      case (terms, _) => terms.map(_._3).sorted
    }.map(_._2.head).toSeq.sortBy {
      case (_, docs) => -docs.size
    }

    (tag, filteredResults)
  }.foreach { case  (tag, results) =>
    println()
    println(s"Printing tag $tag")
    println()
    for ((terms, docs) <- results) {
      println("Docs count: " + docs.size)
      println("Concept terms: " + terms.map(_._1).mkString(" --- "))
      println("Concept docs: " + docs.map(_._1).mkString(" --- "))
      println()
    }
  }
}
