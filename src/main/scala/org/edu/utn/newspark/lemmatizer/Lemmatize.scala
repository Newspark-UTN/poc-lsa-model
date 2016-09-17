package org.edu.utn.newspark.lemmatizer

import org.bson.types.ObjectId
import org.edu.utn.newspark.provider.MongoNewsProvider

final case class NewsMeta(id: ObjectId, title: String, tag: String, imageUrl: String)
final case class News(meta: NewsMeta, content: String)
final case class MongoContent(content: String, link: String, title: String, tag: String, source: String, imageUrl: String, _id: ObjectId) {
  def toNews = News(NewsMeta(_id, title, tag, imageUrl), content)
}
sealed trait Group {
  def active: Boolean
  def concepts: Seq[String]
  def news: Seq[ObjectId]
  def image: String
  def category: String
}
final case class NonPersistedMongoGroup(concepts: Seq[String], news: Seq[ObjectId], image: String, category: String, active: Boolean = false) extends Group
final case class PersistedMongoGroup(id: ObjectId, concepts: Seq[String], news: Seq[ObjectId], image: String, category: String, active: Boolean) extends Group
