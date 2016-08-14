package org.edu.utn.newspark

import java.util.Properties

import scala.io.Source
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * PoC object that cleans and tokenizes incoming news using Core NLP.
  *
  * @author tom
  */
final case class News(title: String, content: String)

object Lemmatizer extends App {

  val newsFile = Source.fromInputStream(getClass.getResourceAsStream("/news.txt"))
  val stopWordsFile = Source.fromInputStream(getClass.getResourceAsStream("/stopwords.txt"))
  val newsFromFile = try newsFile.getLines.map(_.toLowerCase).toList finally newsFile.close()
  val stopwords = try stopWordsFile.getLines.toSet finally stopWordsFile.close()

  // Take two lines at a time to form (title, content)
  val news: List[News] =
    newsFromFile
      .grouped(2)
      .map{ case title :: content :: Nil => News(title, content) }
      .toList

  val LemmaPipeline = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
    props.setProperty("tokenize.language", "es")
    props.setProperty("pos.model", "edu/stanford/nlp/models/pos-tagger/spanish/spanish-distsim.tagger")
    new StanfordCoreNLP(props)
  }

  def isOnlyLetters(s: String) = s forall Character.isLetter

  // output all tokens as lowercase after passing through the nlp pipeline
  def plainTextToLemmas(content: String, stopwords: Set[String]): mutable.Buffer[String] = {
    val doc = new Annotation(content)
    LemmaPipeline.annotate(doc)

    val sentences = doc.get(classOf[SentencesAnnotation])

    for {
      sentence <- sentences.asScala
      token <- sentence.get(classOf[TokensAnnotation]).asScala
      lemma = token.get(classOf[LemmaAnnotation])
      if lemma.length > 2 && !stopwords.contains(lemma) && isOnlyLetters(lemma)
    } yield lemma.toLowerCase
  }

  // place lemmas together with their news again
  news collect { case News(title, content) => News(title, plainTextToLemmas(content, stopwords) mkString " ") } foreach println
}
