name:= "poc-lsa-model"

version:= "0.0.1-SNAPSHOT"

scalaVersion:= "2.11.8"

val cleaningDependencies = Seq("edu.stanford.nlp" % "stanford-corenlp" % "3.6.0")

libraryDependencies ++= cleaningDependencies
