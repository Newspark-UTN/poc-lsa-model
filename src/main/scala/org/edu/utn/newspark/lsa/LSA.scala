package org.edu.utn.newspark.lsa

import java.util.Date

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.edu.utn.newspark.lemmatizer.{MongoGroup, MongoGroupContentToRetrieve, News, NewsMeta}
import org.edu.utn.newspark.provider.{MongoGroupDAO, MongoNewsDAO}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LSA extends MongoNewsDAO with App {
  // We need the number of concepts to make the SVD, else set it to default.
  val K = if (args.length > 0) args(0).toInt else 100
  val topConcepts = if (args.length > 1) args(1).toInt else 30
  val topTerms = if (args.length > 2) args(2).toInt else 4
  val topDocuments = if (args.length > 3) args(3).toInt else 30
  val minTermMatchPerDocumentPercentage = if (args.length > 4) args(4).toDouble else 1.0

  if (minTermMatchPerDocumentPercentage <= 0 || minTermMatchPerDocumentPercentage > 1) throw new IllegalArgumentException("minTermMatchPerDocumentPercentage must between 0 and 1")

  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("LSA Newspark")
  val sc = new SparkContext(conf)

  val allNews: List[News] = retrieveNews

  // For each tag, calculate svd, termIds, and docIds
  val LSARuns: Seq[(SVD, Tag, Map[Term, Index], Map[Long, NewsMeta], Map[Long, mutable.HashMap[String, Int]])] =

    allNews.groupBy(_.meta.tag).map { case (tag, newsPerTag) =>

      val numDocs = newsPerTag.length

      // Lift to a parallel RDD
      val withoutLemmaRDD: RDD[News] = sc.parallelize(newsPerTag)

      // Clean all the documents
      val lemmatizedRDD: RDD[(NewsMeta, Seq[String])] = withoutLemmaRDD collect { case News(meta, content) => (meta, plainTextToLemmas(content, stopwords)) }

      val docTermFrequencies: RDD[(NewsMeta, mutable.HashMap[String, Int])] = lemmatizedRDD.mapValues(terms => {
        val termFreqsInDoc = terms.foldLeft(new mutable.HashMap[String, Int]()) {
          (map, term) => map += term -> (map.getOrElse(term, 0) + 1)
        }
        termFreqsInDoc
      })

      // We will cache this in memory since we are going to use it 2 times:
      // to calculate idfs and term document matrix
      docTermFrequencies.cache()

      // Identify each news meta with an id for later usage
      val docIds: Map[Long, NewsMeta] = docTermFrequencies.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap().toMap
      // Identify also each word count (each document) with an id for later usage on grouping
      val docContent: Map[Long, mutable.HashMap[String, Int]] = docTermFrequencies.map(_._2).zipWithUniqueId().map(_.swap).collectAsMap().toMap

      /** CALCULATION OF DOCUMENT FREQUENCIES */

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
      def mergeDocumentFrequencies(dfs: mutable.HashMap[Term, Int], tfs: (NewsMeta, mutable.HashMap[String, Int])): mutable.HashMap[String, Int] = {
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

      // Next, create a document frequency rdd of the form (term, number of documents in which the term appears)
      val documentFrequencies: mutable.HashMap[Term, Int] = docTermFrequencies.aggregate(zero)(mergeDocumentFrequencies, combinePartialDocumentFrequencies)

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

      (svd, tag, termIds, docIds, docContent)
    }.toSeq

    def docsWhichContainsTerms(results: Seq[(Seq[(String, Double, Int)], Seq[(NewsMeta, Double, Long)])],
                               documents:  Map[Long, mutable.HashMap[String, Int]])
    : Seq[(Seq[(String, Double, Int)], Seq[(NewsMeta, Double, Long)])] = {
      results.map {
        case (terms, docs) =>
          val filterDocs = docs.filter {
            case (_, _, id) =>
              val frequencies = documents.getOrElse(id, new mutable.HashMap[String, Int]())
              val termsCountInContent = terms.count {
                case (term, _, _) => frequencies.contains(term)
              }

              val percentageOfTermsContained = termsCountInContent / topTerms.toDouble

              percentageOfTermsContained >= minTermMatchPerDocumentPercentage
          }
          (terms, filterDocs)
      }
    }

  /**
   * Gets hold of V (m, k) matrix, m being the number of terms of the tag corpus and k the number of concepts considered,
   * and ranks the top `numTerms` in top `numConcepts`, returning their value, score for those concepts and id inside a
   * term map to be accessed later.
   *
   * @param svd the singular value decomposition matrices for the tag
   * @param numConcepts the number of concepts to take into account when ranking
   * @param numTerms the number of terms to rank
   * @param termIds the map of ids against terms
   * @return and array of arrays with the term scores ranked
   */
    def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix],
                              numConcepts: Int, numTerms: Int, termIds: Map[Long, String])
    : Seq[Seq[TermScore]] = {
      val v = svd.V
      val topTerms = new ArrayBuffer[Seq[TermScore]]()
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

  /**
   * Gets hold of U (k, n) matrix, k being the number of concepts and n the number of documents for the current tag,
   * creating an array of arrays with the meta data of the news, its score against those top `numConcepts` and id
   * inside a map of documents for later usage
   *
   * @param svd the singular value decomposition matrices for the tag
   * @param numConcepts the number of concepts to consider when ranking
   * @param numDocs the number of documents to rank
   * @param docIds the map to access when going over the U matrix, to get the meta data of the news
   * @return an array of arrays with the top docs
   */
    def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix],
                             numConcepts: Int, numDocs: Int, docIds: scala.collection.Map[Long, NewsMeta])
    : Seq[Seq[DocScore]] = {
      val u = svd.U
      val topDocs = new ArrayBuffer[Seq[DocScore]]()
      for (i <- 0 until numConcepts.min(u.numRows().toInt)) {
        val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
        topDocs += docWeights.top(numDocs).map {
          case (score, id) => (docIds(id), score, id)
        }
      }
      topDocs
    }

  val mongoDAO = new MongoGroupDAO
  val persistedGroups: List[MongoGroupContentToRetrieve] = mongoDAO.retrieve

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
    }.map {
      // Retrieve image url, if available
      case (term, docs) =>
        val imageLink = docs.find(_._1.imageUrl != "").fold("")(doc => doc._1.imageUrl)
        (term, docs, imageLink, false)
    }

    (tag, filteredResults)
  }.foreach { case  (tag, results) =>
    println()
    println(s"Printing tag $tag")
    println()
    val persistedMongoGroupForTag = persistedGroups.filter(group => group.category == tag)
    val persistedGroupForTag = persistedMongoGroupForTag.map(_.toGroup)
    val withoutDuplicates: Seq[Group] = DuplicateAnalyzer.removeDuplicates(results ++ persistedGroupForTag)

    val persistedGroupsAfterRemoveDuplicates = withoutDuplicates.filter(_._4)
    if(persistedGroupsAfterRemoveDuplicates.length != persistedGroupForTag.length) {
      println(s"******************************************************")
      println(s"******************************************************")
      println(s"******************************************************")
      println(s"${(persistedGroupForTag diff persistedGroupsAfterRemoveDuplicates).map(group => group._3)}")
      println(s"******************************************************")
      println(s"******************************************************")
      println(s"******************************************************")
    }

    // Results coming as (concepts, docs in group, image)
    for (group @ (terms, docs, image, persisted) <- withoutDuplicates if !persisted) {
      if (docs.nonEmpty) {
        println("Docs count: " + docs.size)
        println("Concept terms: " + terms.map(_._1).mkString(" --- "))
        println("Concept docs: " + docs.map(_._1.title).mkString(" --- "))
        println()
      }

      mongoDAO.save(lsaGroupToMongoGroup(group, tag))
    }
  }

  /**
   * Group adapter to MongoGroup
   *
   * @param group the group to adapt
   * @param tag the tag of the group
   * @return the corresponding MongoGroup
   */
  def lsaGroupToMongoGroup(group: Group, tag: String): MongoGroup = {
    val (terms, docs, image, persisted) = group
    val concepts = terms.map(_._1)
    val newsObjectIds = docs.map(_._1.id)
    val newsDates = docs.map(_._1.date)
    MongoGroup(
      concepts = concepts,
      news = newsObjectIds,
      image = image,
      category = tag,
      minDate = newsDates.min,
      maxDate = newsDates.max,
      groupedDate = new Date,
      group = group
    )
  }
}
