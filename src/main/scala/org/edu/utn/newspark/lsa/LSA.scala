package org.edu.utn.newspark.lsa

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.edu.utn.newspark.lemmatizer.News
import org.edu.utn.newspark.provider.MongoNewsProvider

object LSA extends MongoNewsProvider with App {
  val conf = new SparkConf().setAppName("LSA Newspark")
  val sc = new SparkContext(conf)

  val withoutLemmaRDD: RDD[News] = sc.parallelize(retrieveNews)

  val lemmatizedRDD: RDD[Seq[String]] = withoutLemmaRDD collect { case News(_, content) => plainTextToLemmas(content, stopwords) }

  lemmatizedRDD.collect.foreach(println)
}
