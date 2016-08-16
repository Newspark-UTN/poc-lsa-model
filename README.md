# poc-lsa-model

## Installation (local)
- Download and install [Scala](http://www.scala-lang.org) and [SBT](http://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Mac.html)
- Install [MongoDB](https://docs.mongodb.com/manual/installation/) to store the data.
  Once installed, use the `mongod` command to start mongo.
  Create a new db named `newspark` with collection `news` and user `newspark` by:

  - `mongo`
  - `use newspark`
  - `db.createCollection("news")`
  - ```
     db.createUser(
        {
          user: "newspark",
          pwd: "newspark",
          roles: [ "readWrite", "dbAdmin" ]
        }
     )
     ```

- This PoC relies on [Spark](http://spark.apache.org)'s [MLlib](http://spark.apache.org/mllib/), so go ahead and install it. You can use Tom's simple but effective [installation guide](https://github.com/tomduhourq/dotfiles/blob/master/install/spark)
- Go to the root of the project and run `sbt gen-idea` (if using Intellij IDEA)

## Optional - Test data

  To store some test data:

- Outside the console `mongoimport --db newspark --collection news --drop --file path-to-repo/poc-lsa-model/src/main/resources/realdata/2016-08-15.json`

## Tests

### Lemmatization
- `sbt run` and select `Lemmatizer`
