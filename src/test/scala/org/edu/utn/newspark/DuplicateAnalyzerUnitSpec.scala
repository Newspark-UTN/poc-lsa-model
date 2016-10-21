package org.edu.utn.newspark

import org.edu.utn.newspark.lemmatizer.NewsMeta
import org.edu.utn.newspark.utils.NewsFixture
import org.specs2.mutable.Specification

import scala.collection.mutable

class DuplicateAnalyzerUnitSpec extends Specification with NewsFixture {
  import org.edu.utn.newspark.lsa.DuplicateAnalyzer._

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

      maxTermScores must have size 3
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
