name:= "poc-lsa-model"

version:= "0.0.1-SNAPSHOT"

scalaVersion:= "2.11.8"


val persistenceDependencies = Seq(
  "org.mongodb" %% "casbah" % "3.1.1",
  "com.novus" % "salat_2.11" % "1.9.9" pomOnly()
)

val testDependencies = Seq(
  "org.specs2" % "specs2-core_2.11" % "3.8.4.1-scalaz-7.1"
)

val cleaningDependencies = Seq(
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models-spanish"
)

libraryDependencies ++= cleaningDependencies ++ testDependencies ++ persistenceDependencies
