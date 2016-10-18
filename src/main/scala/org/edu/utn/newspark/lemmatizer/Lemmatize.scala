package org.edu.utn.newspark.lemmatizer

import java.util.Date

import org.bson.types.ObjectId

final case class NewsMeta(id: ObjectId, title: String, tag: String, imageUrl: String)
final case class News(meta: NewsMeta, content: String)
final case class MongoContent(content: String, link: String, title: String, tag: String, source: String, imageUrl: String, _id: ObjectId, scrapeDate: Date) {
  def toNews = News(NewsMeta(_id, title, tag, imageUrl), content)
}
final case class MongoGroup(concepts: Seq[String], news: Seq[ObjectId], image: String, category: String)
