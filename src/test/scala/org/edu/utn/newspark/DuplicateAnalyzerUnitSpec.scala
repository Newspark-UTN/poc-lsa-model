package org.edu.utn.newspark

import java.util.Date

import org.bson.types.ObjectId
import org.edu.utn.newspark.lemmatizer.NewsMeta
import org.specs2.mutable.Specification

import scala.collection.mutable

class DuplicateAnalyzerUnitSpec extends Specification {
  import org.edu.utn.newspark.lsa.DuplicateAnalyzer._

  def generateObjId(str: String) = new ObjectId(new Date)
  def generateNews(title: String) = NewsMeta(generateObjId(title), title, "Deportes", "the image url")

  // Fixture

  // News
  val newsMessi1 = generateNews("messi1")
  val newsMessi2 = generateNews("messi2")
  val newsRonaldo = generateNews("ronaldo")

  // Term Scores
  val messiTermScore = ("messi", 100D, 1)
  val futbolTermScore = ("futbol", 75D, 3)

  val termScoresMessi1 = List(messiTermScore, futbolTermScore, ("barcelona", 90D, 2))
  val termScoresMessi1B = List(messiTermScore, futbolTermScore, ("evasion", 110D, 2))
  val termScoresMessi2 = List(messiTermScore, futbolTermScore, ("argentina", 85D, 4))
  val termScoresRonaldo = List(("ronaldo", 100D, 5), futbolTermScore, ("real", 55D, 4))

  // Doc scores
  val docMessi1 = (newsMessi1, 40D, 1L)
  val docMessi1B = (newsMessi1, 50D, 2L)
  val docMessi2 = (newsMessi2, 75D, 3L)
  val docRonaldo = (newsRonaldo, 20D, 4L)

  val docScoresMessi1 = List(docMessi1)
  val docScoresMessi1B = List(docMessi1B)
  val docScoresMessi2 = List(docMessi2)
  val docScoresRonaldo = List(docRonaldo)

  // Groups
  val messiGroup1 = (termScoresMessi1, docScoresMessi1, newsMessi1.imageUrl)
  val messiGroup1B = (termScoresMessi1B, docScoresMessi1B, newsMessi1.imageUrl)
  val messiGroup2 = (termScoresMessi2, docScoresMessi2, newsMessi2.imageUrl)
  val ronaldoGroup = (termScoresRonaldo, docScoresRonaldo, newsRonaldo.imageUrl)

  val groups = List(messiGroup1, messiGroup1B, messiGroup2, ronaldoGroup)

  val invertedIndex = invertedMetaConceptScoreIndex(groups)
  val maxTermScores = invertedToMaxTermScores(invertedIndex)

  "A request to get an inverted index of documents against scores" should {
    "return the map correctly" in {
      val scoreMessi1 = invertedIndexValue(termScoresMessi1, docScoresMessi1)
      val scoreMessi1B = invertedIndexValue(termScoresMessi1B, docScoresMessi1B)
      val scoreMessi2 = invertedIndexValue(termScoresMessi2, docScoresMessi2)
      val scoreRonaldo = invertedIndexValue(termScoresRonaldo, docScoresRonaldo)

      val expectedValues = List(scoreMessi1, scoreMessi1B, scoreMessi2, scoreRonaldo)

      invertedIndex must haveKeys(newsMessi1, newsMessi2, newsRonaldo)
      invertedIndex(newsMessi1) must haveSize(2) // Test duplicate
      invertedIndex.values.flatten must containTheSameElementsAs(expectedValues)
    }
    "return an empty map if no groups" in {
      invertedMetaConceptScoreIndex(Seq()) must beEmpty
    }
  }

  "A request to get the document terms by max score map" should {
    "return the max for the tuples" in {
      val (terms, _) = invertedIndexValue(termScoresMessi1B, docScoresMessi1B)

      maxTermScores must have size 1
      maxTermScores.values.head must beEqualTo(terms)
    }
    "return empty for an empty inverted index" in {
      invertedToMaxTermScores(new mutable.HashMap[NewsMeta, List[(Seq[String], Double)]]()) must beEmpty
    }
  }

  "A request to keep only the groups containing the max value set of terms" should {
    "filter the lesser ones out" in {
      val potentialGroups = List(messiGroup1, messiGroup1B)
      keepOnlyWithMaxScore(potentialGroups, maxTermScores).head must beEqualTo(messiGroup1B)
    }
  }

  /** Replicates the elements of the map returned by the inverted index function. */
  def invertedIndexValue(termScores: List[(String, Double, Int)], docScores: List[(NewsMeta, Double, Long)]): (List[String], Double) =
    (termScores.map(_._1), termScores.map(_._2).sum + docScores.map(_._2).sum)
}
