package org.edu.utn.newspark.provider

import org.edu.utn.newspark.lemmatizer.{MongoContent, News}

import scala.io.Source

/**
 * Generic news provider
 */
trait Stopwords {
  private val stopWordsFile = Source.fromInputStream(getClass.getResourceAsStream("/stopwords.txt"))
  val stopwords = try stopWordsFile.getLines.toSet finally stopWordsFile.close()
}

sealed trait NewsProvider extends Stopwords {
  def retrieveNews: List[News]
}

class FileNewsProvider extends NewsProvider {
  val newsFile = Source.fromInputStream(getClass.getResourceAsStream("/fakedata/news.txt"))
  val newsFromFile = try newsFile.getLines.map(_.toLowerCase).toList finally newsFile.close()

  def retrieveNews =
    newsFromFile
      .grouped(2)
      .map{ case title :: tag :: content :: Nil => News(title, tag, content) }
      .toList
}

class MongoNewsProvider extends NewsProvider {
  import com.mongodb.casbah.Imports._
  import com.novus.salat._
  import com.novus.salat.annotations._
  import com.novus.salat.global._
  val mongoClient =  MongoClient("mongo.newspark.local", 27017)
  val db = mongoClient("newspark")
  val news = db("news")

  val allDocs = news.find()
  override def retrieveNews: List[News] = allDocs.map(obj => grater[MongoContent].asObject(obj).toNews).toList
}