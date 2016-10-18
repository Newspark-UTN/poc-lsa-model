package org.edu.utn.newspark.lsa

import org.edu.utn.newspark.lemmatizer.NewsMeta

import scala.collection.mutable

/**
 * Used for implementing different duplication removal strategies.
 */
object DuplicateAnalyzer {

  /**
   * Removes duplicates by creating and index of documents against groups they belong with a score.
   * Then, the max of those scores is kept.
   * Finally, the groups are split among the ones that are contained in the index to be removed and those that do not.
   * The first group is filtered to be kept with only the ones that are equal to the max score.
   *
   * @param groups the groups to remove duplicates
   * @return the same group result with removed duplicates
   */
  def removeDuplicates(groups: Seq[Group]): Seq[Group] = {
    val documentInvertedIndex: mutable.HashMap[NewsMeta, List[(Seq[Term], Double)]] = invertedMetaConceptScoreIndex(groups)
    val maxScoreTermsForNews: mutable.HashMap[NewsMeta, Seq[Term]] = invertedToMaxTermScores(documentInvertedIndex)

    val (groupsToSelectMaxScore, groupsToKeepIntact) = groups.partition{ case (_, docs, _) =>
      docs.map(_._1).exists(maxScoreTermsForNews.keySet.contains)
    }

    val withoutDuplicates: Seq[Group] = keepOnlyWithMaxScore(groupsToSelectMaxScore, maxScoreTermsForNews)

    withoutDuplicates ++ groupsToKeepIntact
  }

  /**
   * Creates a Map of (document, [(concepts, score)], with score = termScores + docScore, making it equitative
   * for the groups to evaluate not only from one score.
   *
   * @param groups the already generated groups with potential duplicates
   * @return the aofrementioned map
   */
  def invertedMetaConceptScoreIndex(groups: Seq[Group]): mutable.HashMap[NewsMeta, List[(Seq[Term], Double)]] = {
    groups.foldLeft(new mutable.HashMap[NewsMeta, List[(Seq[Term], Double)]]()) {
      case (actualMap: mutable.HashMap[NewsMeta, List[(Seq[Term], Double)]], group: Group) =>
        val (termScores, docScores, image) = group
        val documents: Seq[NewsMeta] = docScores.map(_._1)
        documents.foreach { case meta: NewsMeta =>
          val scoreToAdd: (Seq[Term], Double) = (termScores.map(_._1), termScores.map(_._2).sum + docScores.map(_._2).sum)
          actualMap += meta -> (scoreToAdd :: actualMap.getOrElse(meta, Nil))
        }
        actualMap
    }
  }

  /**
   * Converts an inverted index of (document, List((terms, score)) to a simple map of (document, terms),
   * selecting the max by the score
   *
   * @param invertedIndex the input inverted index
   * @return the (document, terms) map
   */
  def invertedToMaxTermScores(invertedIndex: mutable.HashMap[NewsMeta, List[(Seq[Term], Double)]]): mutable.HashMap[NewsMeta, Seq[Term]] = {
    invertedIndex collect {
      case (newsMeta, conceptScores) if conceptScores.length > 1 =>
        val (maxScoreTermGroup, _) = conceptScores.maxBy{ case (_, score) => score }
        newsMeta -> maxScoreTermGroup
    }
  }

  /**
   * Checks if the documents inside the groups are the same as the terms (which were the ones with max value)
   * and filters them out, keeping only the one that matches the value of the given news.
   *
   * @param potentialGroups groups to be filtered
   * @param maxScores the map of (news, termsWithMaxScore)
   * @return the filtered group collection without duplicates
   */
  def keepOnlyWithMaxScore(potentialGroups: Seq[Group], maxScores: mutable.HashMap[NewsMeta, Seq[String]]): Seq[Group] = {
    val newsToKeep = maxScores.keySet
    potentialGroups.filter { case (terms, docs, _) =>
      docs
        .find{ case (meta, _, _) => newsToKeep.contains(meta)}
        .flatMap{ case (meta, _, _) => maxScores.get(meta)}
        .contains(terms.map(_._1))
    }
  }
}
