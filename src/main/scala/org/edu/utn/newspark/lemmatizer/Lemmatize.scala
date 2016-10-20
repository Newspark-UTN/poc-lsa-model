package org.edu.utn.newspark.lemmatizer

import java.util.Date

import com.novus.salat.annotations._
import org.bson.types.ObjectId
import org.edu.utn.newspark.lsa.Group

final case class NewsMeta(id: ObjectId, title: String, tag: String, imageUrl: String, date: Date)
final case class News(meta: NewsMeta, content: String)
final case class MongoContent(content: String, link: String, title: String, tag: String, source: String, imageUrl: String, _id: ObjectId, scrapeDate: Date) {
  def toNews = News(NewsMeta(_id, title, tag, imageUrl, scrapeDate), content)
}


final case class MongoGroup(concepts: Seq[String], news: Seq[ObjectId], image: String, category: String, minDate: Date, maxDate: Date, groupedDate: Date, group: Group)

// Group support

// Plain representation of the (concept, score, index) tuple from a Group
final case class ConceptScore(concept: String, score: Double, index: Int)

object ConceptScore {
  def fromGroup(group: Group): Seq[ConceptScore] = group._1.map { case (concept, score, index) => ConceptScore(concept, score, index) }
}

// Plain representation of (docNewsMeta, score, index) from a Group
final case class DocScore(id: ObjectId, title: String, tag: String, imageUrl: String, date: Date, score: Double, index: Long)

object DocScore {
  def fromGroup(group: Group): Seq[DocScore] = group._2.map {
    case (NewsMeta(id, title, tag, imageUrl, date), score, index) =>
      DocScore(id, title, tag, imageUrl, date, score, index)
  }
}

final case class MongoGroupContent(
  concepts: Seq[String],
  category: String,
  image: String,
  articles: Seq[ObjectId],
  viewsCount: Int,
  articlesCount: Int,
  minDate: Date,
  maxDate: Date,
  groupedDate: Date,
  conceptScores: Seq[ConceptScore],
  docScores: Seq[DocScore]
) {
  def toGroup: Group =
    (
      conceptScores.map(ConceptScore.unapply(_).get),
      docScores.map{ doc =>
        val (id, title, tag, imageUrl, date, score, index) = DocScore.unapply(doc).get
        (NewsMeta(id, title, tag, imageUrl, date), score, index)
      },
      image
    )
}
