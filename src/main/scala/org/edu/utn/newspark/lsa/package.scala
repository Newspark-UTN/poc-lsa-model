package org.edu.utn.newspark

import java.lang.Character.isLetter
import java.text.Normalizer
import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}

import scala.collection.JavaConverters._
import scalaz.syntax.std.boolean._

/**
 * We should place functions that will likely be used across the project.
 */
package object lsa {

  /**
   * Function that takes a word and removes the "es" and "s" for the moment
   * naively to conform a rooting technique
   *
   * Example: "Marcadores" will be rooted to "marcador"
   */
  val lowerAndRootWord = (word: String) => {
    val lowered = word.toLowerCase
    lowered.endsWith("es").fold(lowered.dropRight(2), lowered)
  }

  /**
   * Removes accents from words, which are really common in Spanish.
   *
   * Examples: "ñoqui" will be "noqui", "tirabuzón" will be "tirabuzon"
   */
  val extractAccents = (word: String) => Normalizer.normalize(word, Normalizer.Form.NFD).replaceAll("\\p{M}", "")

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
      lemma = (lowerAndRootWord andThen extractAccents).apply(token.get(classOf[LemmaAnnotation]))
      if lemma.length > 2 && !stopwords.contains(lemma) && isOnlyLetters(lemma)
    } yield lemma
  }
}
