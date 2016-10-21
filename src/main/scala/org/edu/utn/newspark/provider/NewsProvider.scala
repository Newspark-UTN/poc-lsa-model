package org.edu.utn.newspark.provider

import java.util.Date

import com.mongodb.casbah.Imports._
import org.edu.utn.newspark.lemmatizer._
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
//val mongoClient = MongoClient("mongo.newspark.local", 27017)
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

  override val collection = db("news")

  // Query news that are >= 1 day before today
  def oneDayBeforeTodayQuery = "scrapeDate" $gte new Date().addDays(-1)
  val allDocs = collection.find(oneDayBeforeTodayQuery)
  override def retrieveNews: List[News] = allDocs.map(obj => grater[MongoContent].asObject(obj).toNews).toList
}

object MongoGroupDAO {
  implicit val mongoGroupMapper : MongoGroup => DBObject = mongoGroup =>
    MongoDBObject(
      "concepts" -> mongoGroup.concepts,
      "category" -> mongoGroup.category,
      "image" -> mongoGroup.image,
      "articles" -> mongoGroup.news,
      "viewsCount" -> 0,
      "articlesCount" -> mongoGroup.news.size,
      "minDate" -> mongoGroup.minDate,
      "maxDate" -> mongoGroup.maxDate,
      "groupedDate" -> mongoGroup.groupedDate,
      "conceptScores" -> ConceptScore.fromGroup(mongoGroup.group),
      "docScores" -> DocScore.fromGroup(mongoGroup.group)
    )
}

class MongoGroupDAO extends MongoConfiguration {
  import MongoGroupDAO._
  import com.mongodb.casbah.Imports._
  import com.novus.salat._
  import com.novus.salat.global._

  override val collection = db("groups")

  def retrieve: List[MongoGroupContentToRetrieve] = collection.find().map(obj => grater[MongoGroupContentToRetrieve].asObject(obj)).toList

  def save(group: MongoGroup) = {
    collection.save[MongoGroup](group)
  }
}