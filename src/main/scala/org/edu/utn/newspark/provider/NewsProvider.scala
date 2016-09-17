package org.edu.utn.newspark.provider

import com.mongodb.casbah.Imports._
import org.edu.utn.newspark.lemmatizer._

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

sealed trait NewsProvider extends Stopwords with MongoConfiguration {
  def retrieveNews: List[News]
}

sealed trait GroupsProvider extends MongoConfiguration {
  def retrieveGroups: List[PersistedMongoGroup]
}

class MongoNewsProvider extends NewsProvider {
  import com.mongodb.casbah.Imports._
  import com.novus.salat._
  import com.novus.salat.global._
  def collection = db("news")

  val allDocs = collection.find()
  override def retrieveNews: List[News] = allDocs.map(obj => grater[MongoContent].asObject(obj).toNews).toList
}

class MongoGroupConnector extends GroupsProvider {
  import com.mongodb.casbah.Imports._
  import com.novus.salat._
  import com.novus.salat.global._
  def collection = db("groups")

  implicit val mongoGroupMapper : Group => DBObject = (group) => {
    MongoDBObject(
      "concepts" -> group.concepts,
      "category" -> group.category,
      "image" -> group.image,
      "articles" -> group.news,
      "viewsCount" -> 0,
      "articlesCount" -> group.news.size
    )
  }

  def save(group: Group) = {
    collection.save[Group](group)
  }

  def update(group: PersistedMongoGroup) = {
    val q = MongoDBObject("_id" -> s"ObjectId(${group.id})")
    collection.findAndModify(q, grater[PersistedMongoGroup].asDBObject(group))
  }

  // Retrieve only the ones which are active
  def retrieveGroups: List[PersistedMongoGroup] = {
    val q = MongoDBObject("active" -> "true")
    collection.find(q).map(obj => grater[PersistedMongoGroup].asObject(obj)).toList
  }
}