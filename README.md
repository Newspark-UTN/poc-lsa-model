# poc-lsa-model

## Installation
- Download and install [Scala](http://www.scala-lang.org) and [SBT](http://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Mac.html)
- This PoC relies on [Spark](http://spark.apache.org)'s [MLlib](http://spark.apache.org/mllib/), so go ahead and install it. You can use Tom's simple but effective [installation guide](https://github.com/tomduhourq/dotfiles/blob/master/install/spark)
- Go to the root of the project and run `sbt gen-idea` (if using Intellij IDEA)

## Tests

### Lemmatization
- `sbt run` and select `Lemmatize`
