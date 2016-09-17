package org.edu.utn.newspark.provider

import com.mongodb.casbah.Imports._
import org.bson.types.ObjectId
import org.edu.utn.newspark.lemmatizer.{MongoContent, MongoGroup, News}

import scala.io.Source

/**
 * Generic news provider
 */
trait Stopwords {
  private val stopWordsFile = Source.fromInputStream(getClass.getResourceAsStream("/stopwords.txt"))
  val stopwords = try stopWordsFile.getLines.toSet finally stopWordsFile.close()
}

sealed trait MongoConfiguration {
  val mongoClient =  MongoClient("mongo.newspark.local", 27017)
  val db = mongoClient("newspark")
  def collection: MongoCollection
}

sealed trait NewsProvider extends Stopwords {
  def retrieveNews: List[News]
}

class MongoNewsProvider extends NewsProvider with MongoConfiguration {
  import com.mongodb.casbah.Imports._
  import com.novus.salat._
  import com.novus.salat.annotations._
  import com.novus.salat.global._
  def collection = db("news")

  val allDocs = collection.find()
  override def retrieveNews: List[News] = allDocs.map(obj => grater[MongoContent].asObject(obj).toNews).toList
}

class MongoGroupSaver extends MongoConfiguration {
  def collection = db("groups")

  implicit val mongoGroupMapper : MongoGroup => DBObject = (group) => {
    MongoDBObject(
      "concepts" -> group.concepts,
      "category" -> group.category,
      "image" -> group.image,
      "articles" -> group.news,
      "viewsCount" -> 0
    )
  }

  def save(group: MongoGroup) = {
    collection.save[MongoGroup](group)
  }
}