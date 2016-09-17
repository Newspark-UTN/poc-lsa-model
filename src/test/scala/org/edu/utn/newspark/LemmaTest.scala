package org.edu.utn.newspark

import org.specs2.mutable.Specification
import org.edu.utn.newspark.lsa._
import org.edu.utn.newspark.provider.Stopwords

class LemmaTest extends Specification with Stopwords {

  "Our lemmatizer" should {
    "extract accents correctly" in {
      extractAccents("ñoquí") ==== "noqui"
    }
    "lower and root words correctly" in {
      lowerAndRootWord("MARCadores") ==== ("marcadore")
    }
    "detect that a word is fully composed of letters" in {
      val word = "Holaqhace"
      isOnlyLetters(word) should beTrue
    }
    "detect that a word is not composed fully of letters" in {
      val incorrect = "112a"
      isOnlyLetters(incorrect) should beFalse
    }
    "Turn a single line into an array of terms" in {
      val sentence = "El presidente de los Estados Unidos visita la Argentina"
      plainTextToLemmas(sentence, stopwords) should contain(allOf(Set("presidente", "estados", "unidos", "visita", "argentina")))
    }
  }
}
