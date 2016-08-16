package org.edu.utn.newspark

import org.edu.utn.newspark.provider.{FileNewsProvider, MongoNewsProvider}

/**
  * PoC object that cleans and tokenizes incoming news using Core NLP.
  *
  * @author tom
  */
final case class News(title: String, content: String)
final case class Feed(source: String, link: String, name: String)
final case class MongoContent(title: String, content: String, published: String, author: String, link: String, feed: Feed, contenidoNota: String) {
  def toNews = News(title, contenidoNota)
}

object FileLemmatizer extends FileNewsProvider with App {
  retrieve collect { case News(title, content) => News(title, plainTextToLemmas(content, stopwords) mkString " ") } foreach println
}

object MongoLemmatizer extends MongoNewsProvider with App {
  retrieve collect { case News(title, content) => News(title, plainTextToLemmas(content, stopwords) mkString " ") } foreach println
}
