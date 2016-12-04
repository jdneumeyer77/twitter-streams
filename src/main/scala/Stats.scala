import java.io.File

import cats.data.Xor

import scala.collection.concurrent.TrieMap
import scala.util.Success

object Stats extends nl.grons.metrics.scala.DefaultInstrumented {
  import Utils._

  val totalMessages = metrics.counter("total-messages")
  val totalWarnings = metrics.counter("total-warnings")
  val totalSingleEvents = metrics.counter("total-single-events")
  val totalOtherEvents = metrics.counter("total-other-events")
  val totalUnknownEvents = metrics.counter("total-unknown-events")
  val totalTweetsEmojis = metrics.counter("total-tweets-with-emojis")
  val totalTweetsUrls = metrics.counter("total-tweets-with-a-url")
  val totalTweetsPhotos = metrics.counter("total-tweets-with-a-photo")
  val tweetsMeter = Metrics.meter

  val start = System.nanoTime()

  def countTypes(tweetType: StreamedTweetType): Unit = {
    tweetType match {
      case WarningEvent(_) => totalWarnings.inc()
      case SingleFieldEvent(_) => totalSingleEvents.inc()
      case Event(_) => totalOtherEvents.inc()
      case UnknownEvent(x) => totalUnknownEvents.inc()
      case TweetEvent(_) => tweetsMeter.mark()
    }
  }

  def percentOfTweets(comparedTo: Long): Double = {
    (comparedTo.toDouble / tweetsMeter.count) * 100.0
  }

  private val noItems = Seq("No top items to display (map was empty)!")
  private def top(map: TrieMap[String,Long], count: Int = 5) = {
    if(map.nonEmpty) {
      map.top(5).map {
        case (key, count) => s"$key: $count"
      }
    } else noItems
  }

  //// media
  private val photoDomains = Array("pic.twitter", "instagram", "imgur")
  def collectPhotoUrls(tweet: Tweet): Unit = {
    val containsPhotoUrl_? = tweet.entities.media.exists { media =>
      media.exists { photo =>
        domainFrom(photo.expanded_url).map(photoDomains.contains).getOrElse(false)
      }
    }

    if(containsPhotoUrl_?) totalTweetsPhotos.inc()
  }

  //// urls
  private val urlHostnames = new TrieMap[String,Long]()
  def collectUrls(tweet: Tweet): Unit = {
    if(tweet.entities.urls.nonEmpty) {
      totalTweetsUrls.inc()
      tweet.entities.urls.foreach {
        case Url(expanded_url) =>
          domainFrom(expanded_url).foreach(urlHostnames.addOrIncr)
      }
    }
  }

  def top5Urls = top(urlHostnames)

  //// hashtags
  private val hashTags = new TrieMap[String,Long]()
  def collectHashTags(tweet: Tweet): Unit = {
    tweet.entities.hashtags.foreach(tag => hashTags.addOrIncr(tag.text))
  }

  def top5HashTags: Seq[String] = top(hashTags)


  //// languages
  private val languages = new TrieMap[String,Long]()
  def collectLanguages(tweet: Tweet): Unit = {
    tweet.lang.foreach(languages.addOrIncr)
  }

  def top5Languages: Seq[String] = top(languages)

  //// countires
  private val countries = new TrieMap[String,Long]()
  def collectCountries(tweet: Tweet): Unit = {
    tweet.place.foreach(country => countries.addOrIncr(country.country_code))
  }

  def top5Countries: Seq[String] = top(countries)

  //// emojis
  def top5Emojis: Seq[String] = top(emojis)

  private val emojis = new TrieMap[String,Long]()
  def collectEmojis(emojiData: Map[String,String])(tweet: Tweet): Unit = {
    val found  = tweet.text.foldLeft(false) {
      case (emojiFound, char) =>
        emojiData.get(char.toString) match {
          case Some(shortname) =>
            emojis.addOrIncr(shortname)
            true
          case None => emojiFound
        }
    }

    if(found) totalTweetsEmojis.inc()
  }

  // convert hex string to utf-16 string.
  private def hexStringToUnicode(hexStrings: String): Option[String] = {
    try {
      val ints = hexStrings.split('-').map(x => Integer.parseInt(x, 16))
      Some(new String(ints, 0, ints.length))
    } catch {
      case e: Throwable =>
        println(s"Failed to convert to $hexStrings to string. reason: ${e.getCause}")
        None
    }
  }

  def readEmojiData(name: String): Map[String,String] = {
    val file = new File(name)
    if(!file.exists()) {
      println(s"Couldn't open $name! Tried the follow path: ${file.getAbsolutePath}")
      Map.empty[String,String]
    } else {
      import io.circe.generic.semiauto._

      implicit val emojiDecoder = deriveDecoder[EmojiEntry]

      io.circe.jawn.CirceSupportParser.parseFromFile(file).map { x =>
        x.as[List[EmojiEntry]]
      } match {
        case Success(Xor.Right(emojisDecoded)) =>
          println(s"collecting emoji data. ${emojisDecoded.size}")
          emojisDecoded.foldLeft(Map.newBuilder[String,String]) {
            case (acc, emoji) =>
              hexStringToUnicode(emoji.unified).foreach(emojiUnified => acc += emojiUnified -> emoji.short_name)
              Seq[Option[String]](emoji.au, emoji.google, emoji.docomo, emoji.softbank)
                .flatten
                .flatMap(hexStringToUnicode)
                .foreach(emojiSymbol => acc += emojiSymbol -> emoji.short_name)

              acc
          }.result()
        case fail =>
          println(s"failed to parse emojis: $fail")
          Map.empty[String,String]
      }
    }

  }

  case class EmojiEntry(unified: String, docomo: Option[String],
                        au: Option[String], softbank: Option[String],
                        google: Option[String], short_name: String)
}
