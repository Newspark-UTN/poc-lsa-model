package org.edu.utn.newspark.provider

import org.edu.utn.newspark.{MongoContent, News}

import scala.io.Source

/**
 * Generic news provider
 */
sealed trait NewsProvider {
  private val stopWordsFile = Source.fromInputStream(getClass.getResourceAsStream("/stopwords.txt"))
  val stopwords = try stopWordsFile.getLines.toSet finally stopWordsFile.close()
  def retrieve: List[News]
}

class FileNewsProvider extends NewsProvider {
  val newsFile = Source.fromInputStream(getClass.getResourceAsStream("/fakedata/news.txt"))
  val newsFromFile = try newsFile.getLines.map(_.toLowerCase).toList finally newsFile.close()

  def retrieve =
    newsFromFile
      .grouped(2)
      .map{ case title :: content :: Nil => News(title, content) }
      .toList
}

class MongoNewsProvider extends NewsProvider {
  import com.mongodb.casbah.Imports._
  import com.novus.salat._
  import com.novus.salat.global._
  val mongoClient = MongoClient("localhost", 27017)
  val db = mongoClient("newspark")
  val news = db("news")

  val allDocs = news.find()
  override def retrieve: List[News] = allDocs.map(obj => grater[MongoContent].asObject(obj).toNews).toList
}