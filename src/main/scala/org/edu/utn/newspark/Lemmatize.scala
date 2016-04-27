package org.edu.utn.newspark

import scala.io.Source

/**
  * PoC object that cleans and tokenizes incoming news using Core NLP.
  *
  * @author tom
  */
object Lemmatize extends App {
  val news = Source.fromInputStream(getClass.getResourceAsStream("/news.txt")).getLines.toList
  val stopwords = Source.fromInputStream(getClass.getResourceAsStream("/stopwords.txt")).getLines.toSet

  // TODO (tom): Start lemmatizing and pass on to RDDS in Spark.
  news.view.grouped(4)
    .flatMap(_.filterNot(str => str == "###" || str == "##"))
    .grouped(2)
    .map(tuple => (tuple(0), tuple(1)))
    .foreach(println)
}
