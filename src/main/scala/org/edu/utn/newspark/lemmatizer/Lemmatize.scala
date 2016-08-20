package org.edu.utn.newspark.lemmatizer

import org.edu.utn.newspark.provider.{FileNewsProvider, MongoNewsProvider}

/**
  * PoC object that cleans and tokenizes incoming news using Core NLP.
  *
  * @author tom
  */
final case class News(title: String, content: String)
final case class MongoContent(content: String, link: String, title: String, source: String) {
  def toNews = News(title, content)
}

object FileLemmatizer extends FileNewsProvider {
  val withoutLemmatize = retrieveNews
}

object MongoLemmatizer extends MongoNewsProvider with App {
  retrieveNews foreach println
}
