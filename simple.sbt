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

val sparkDependencies = Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.0.0"
)

libraryDependencies ++= cleaningDependencies ++ testDependencies ++ persistenceDependencies ++ sparkDependencies

assemblyJarName in assembly := "lsa.jar"

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

test in assembly := {}
