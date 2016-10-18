package org.edu.utn.newspark.provider

import java.util.Date

import com.mongodb.casbah.Imports._
import org.edu.utn.newspark.lemmatizer.{MongoContent, MongoGroup, News}
import org.edu.utn.newspark.lsa._

import scala.io.Source

/**
 * Generic news provider
 */
trait Stopwords {
  private val stopWordsFile = Source.fromInputStream(getClass.getResourceAsStream("/stopwords.txt"))
  val stopwords = try stopWordsFile.getLines.toSet finally stopWordsFile.close()
}

trait MongoConfiguration {
  val uri = MongoClientURI("mongodb://admin:newspark@ds033036.mlab.com:33036/newspark")
  val mongoClient =  MongoClient(uri)
//val mongoClient = MongoClient("localhost", 27017)
  val db = mongoClient("newspark")
  def collection: MongoCollection
}

sealed trait NewsProvider extends Stopwords {
  def retrieveNews: List[News]
}

class MongoNewsDAO extends NewsProvider with MongoConfiguration {
  import com.mongodb.casbah.Imports._
  import com.novus.salat._
  import com.novus.salat.global._

  def collection = db("news")

  // Query news that are > 1 day before today
  val oneDayBeforeTodayQuery = "scrapeDate" $gt new Date().addDays(-1)
  val allDocs = collection.find(oneDayBeforeTodayQuery)
  override def retrieveNews: List[News] = allDocs.map(obj => grater[MongoContent].asObject(obj).toNews).toList
}

object MongoGroupDAO {

  implicit val mongoGroupMapper : MongoGroup => DBObject = group =>
    MongoDBObject(
      "concepts" -> group.concepts,
      "category" -> group.category,
      "image" -> group.image,
      "articles" -> group.news,
      "viewsCount" -> 0,
      "articlesCount" -> group.news.size,
      "lsaGroup" -> group
    )
}

class MongoGroupDAO extends MongoConfiguration {
  import MongoGroupDAO._

  def collection = db("groups")

  def save(group: MongoGroup) = {
    collection.save[MongoGroup](group)
  }
}