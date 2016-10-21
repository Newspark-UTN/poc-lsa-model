package org.edu.utn.newspark


import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.global._
import org.edu.utn.newspark.lemmatizer._
import org.edu.utn.newspark.lsa._
import org.edu.utn.newspark.provider.MongoGroupDAO._
import org.edu.utn.newspark.provider.{MongoGroupDAO, MongoNewsDAO}
import org.edu.utn.newspark.utils.NewsFixture
import org.specs2.mutable.Specification
import org.specs2.specification.{AfterAll, BeforeEach}

class MongoItSpec extends Specification with NewsFixture with BeforeEach with AfterAll {

  val twoDaysBeforeNow = now.addDays(-2)

  val newsDAO = new MongoNewsDAO
  val groupsDAO = new MongoGroupDAO

  implicit val MongoContentSerializer: MongoContent => DBObject = content =>
    MongoDBObject(
      "content" -> content.content,
      "link" -> content.link,
      "title" -> content.title,
      "tag" -> content.tag,
      "imageUrl" -> content.imageUrl,
      "source" -> content.source,
      "_id" -> content._id,
      "scrapeDate" -> content.scrapeDate
    )

  // News Fixture
  val mongoContent = MongoContent(
    content = "fancy content that should never change",
    link = "the link",
    title = "Some fancy messi stuff",
    tag = "deportes",
    imageUrl = "MessiUrl",
    source = "clarin",
    _id = generateObjId,
    scrapeDate = now
  )

  val mongoContentTwoDaysAgo = mongoContent.copy(_id = generateObjId, scrapeDate = twoDaysBeforeNow, link = "another link")

  // Group Fixture
  val concepts = Seq("messi", "futbol")
  val newsIds = Seq(generateObjId)
  val image = "the image url"
  val category = "deportes"


  val mongoGroup = MongoGroup(
    concepts = concepts,
    news = newsIds,
    image = image,
    category = category,
    minDate = now,
    maxDate = now,
    groupedDate = now,
    group = messiGroup1
  )

  val mongoGroupContent = MongoGroupContent(
    concepts = concepts,
    category = category,
    image = image,
    articles = newsIds,
    viewsCount = 0,
    articlesCount = 1,
    minDate = now,
    maxDate = now,
    groupedDate = now,
    conceptScores = ConceptScore.fromGroup(messiGroup1),
    docScores = DocScore.fromGroup(messiGroup1)
  )

  def saveMongoContent(mongoContent: MongoContent) = {
    val insert = newsDAO.collection.insert(mongoContent)
    insert.wasAcknowledged() must beTrue
  }

  newsDAO.collection.remove(mongoContent)
  newsDAO.collection.remove(mongoContentTwoDaysAgo)

  groupsDAO.collection.remove(mongoGroup)

  "A request to get news" should {
    "return the inserted news" in {
        saveMongoContent(mongoContent)
        val find = newsDAO.collection.find(MongoDBObject("content" -> mongoContent.content)).map(obj => grater[MongoContent].asObject(obj).toNews).toList
        val news = find.headOption
        news.map(_.content).getOrElse("") ==== mongoContent.content
      }
    "return correctly if we query by date" in {
      saveMongoContent(mongoContent)
      saveMongoContent(mongoContentTwoDaysAgo)
      newsDAO.retrieveNews.filter(_.content == mongoContent.content) must haveSize(1)
    }
  }

  "A request to get groups" should {
    "return the inserted group" in {
      val insert = groupsDAO.save(mongoGroup)
      insert.wasAcknowledged() must beTrue
      groupsDAO.retrieve.headOption.map(_.toGroup) must beSome(mongoGroup.group.copy(_4 = true))
    }
  }

  // Make sure we don't pollute the true db
  override def afterAll(): Unit = {
    newsDAO.collection.remove(mongoContent)
    newsDAO.collection.remove(mongoContentTwoDaysAgo)
    groupsDAO.collection.remove(mongoGroup)
  }

  override protected def before: Any = {
    newsDAO.collection.remove(mongoContent)
    newsDAO.collection.remove(mongoContentTwoDaysAgo)
    groupsDAO.collection.remove(mongoGroup)
  }
}
