package org.edu.utn.newspark.lemmatizer

import java.util.Date

import com.mongodb.BasicDBList
import org.bson.types.{BasicBSONList, ObjectId}
import org.edu.utn.newspark.lsa.{Group, TermScore}

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
)

final case class MongoGroupContentToRetrieve(
  concepts: Seq[String],
  category: String,
  image: String,
  articles: Seq[ObjectId],
  viewsCount: Int,
  articlesCount: Int,
  minDate: Date,
  maxDate: Date,
  groupedDate: Date,
  conceptScores: BasicBSONList,
  docScores: BasicBSONList
) {
  def toGroup: Group =
    (
      {
        val arr = conceptScores.toArray()
        (0 until arr.length).foldLeft(Seq[TermScore]()){ case (acum, i) =>
          val elem = arr(i).asInstanceOf[BasicDBList]
          acum :+ (elem.get(0).asInstanceOf[String], elem.get(1).asInstanceOf[Double], elem.get(2).asInstanceOf[Int])
        }
      }, // TermScores
      {
        val arr = docScores.toArray()
        (0 until arr.length).foldLeft(Seq[org.edu.utn.newspark.lsa.DocScore]()){ case (acum, i) =>
          val elem = arr(i).asInstanceOf[BasicDBList]
          acum :+ (NewsMeta(
            id = elem.get(0).asInstanceOf[ObjectId],
            title = elem.get(1).asInstanceOf[String],
            tag = elem.get(2).asInstanceOf[String],
            imageUrl = elem.get(3).asInstanceOf[String],
            date = elem.get(4).asInstanceOf[Date]),
            elem.get(5).asInstanceOf[Double],
            elem.get(6).asInstanceOf[Long])
        }
      }, // DocScores
      image, // ImageUrl
      true // Persisted
    )
}
