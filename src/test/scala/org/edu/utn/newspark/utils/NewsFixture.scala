package org.edu.utn.newspark.utils

import java.util.Date

import org.bson.types.ObjectId
import org.edu.utn.newspark.lemmatizer.NewsMeta

/**
 * Base fixture trait with news.
 */
trait NewsFixture {

  val now = new Date

  def generateObjId = {
    Thread.sleep(20)
    new ObjectId(new Date)
  }
  def generateNewsMeta(title: String) = NewsMeta(generateObjId, title, "Deportes", "the image url", now)

  // Fixture

  // News
  val newsMessi1 = generateNewsMeta("messi1")
  val newsMessi2 = generateNewsMeta("messi2")
  val newsRonaldo = generateNewsMeta("ronaldo")

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
}
