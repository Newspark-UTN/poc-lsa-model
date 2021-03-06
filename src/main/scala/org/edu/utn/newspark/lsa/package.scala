package org.edu.utn.newspark

import java.lang.Character.isLetter
import java.text.Normalizer
import java.util.{Calendar, Date, Properties}

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.edu.utn.newspark.lemmatizer.NewsMeta

import scala.collection.JavaConverters._

/**
 * We should place functions that will likely be used across the project.
 */
package object lsa {

  type Term = String
  type Tag = String
  type Title = String
  type Id = String
  type ImageUrl = String
  type Index = Int
  type IDF = Double
  type TF_IDF = Double
  type Persisted = Boolean
  type SVD = SingularValueDecomposition[RowMatrix, Matrix]
  type TermScore = (String, Double, Int)
  type DocScore = (NewsMeta, Double, Long)
  type Group = (Seq[TermScore], Seq[DocScore], ImageUrl, Persisted)

  /**
   * Removes accents from words, which are really common in Spanish.
   *
   * Examples: "ñoqui" will be "noqui", "tirabuzón" will be "tirabuzon"
   */
  val extractAccents = (word: String) => Normalizer.normalize(word, Normalizer.Form.NFD).replaceAll("\\p{M}", "").toLowerCase

  /**
   * Checks if a string is conformed fully of letters.
   *
   * @param word the word to analyze
   * @return a boolean telling if this is the case
   */
  def isOnlyLetters(word: String) = word forall isLetter

  /**
   * The cleaning pipeline, uses Stanford Core NLP spanish module.
   */
  val LemmaPipeline = {
    val props = new Properties()
    // We need part-of-speech annotations (and tokenization /
    // sentence-splitting, which are required for POS tagging)
    props.setProperty("annotators", "tokenize,ssplit,pos,lemma")
    props.setProperty("tokenize.language", "es")
    props.setProperty("pos.model", "edu/stanford/nlp/models/pos-tagger/spanish/spanish-distsim.tagger")
    new StanfordCoreNLP(props)
  }

  /**
   * Applies the lemmatization pipeline to the data provided as content.
   *
   * @param content the text to clean.
   * @param stopwords a set of words we don't want inside the text.
   * @return a collection of clean tokens.
   */
  def plainTextToLemmas(content: String, stopwords: Set[String]): Seq[String] = {
    val doc = new Annotation(content)
    LemmaPipeline.annotate(doc)

    val sentences = doc.get(classOf[SentencesAnnotation])

    for {
      sentence <- sentences.asScala
      token <- sentence.get(classOf[TokensAnnotation]).asScala
      lemma = extractAccents.apply(token.get(classOf[LemmaAnnotation]))
      if lemma.length > 2 && !stopwords.contains(lemma) && isOnlyLetters(lemma)
    } yield lemma
  }

  implicit class PimpedDate(val date: Date) {
    def addDays(n: Int): Date = {
      val c = Calendar.getInstance()
      c.setTime(date)
      c.add(Calendar.DATE, n)
      c.getTime
    }
  }

  implicit val DateOrdering = new Ordering[Date] {
    override def compare(x: Date, y: Date): Int = x.getTime.compare(y.getTime)
  }
}
