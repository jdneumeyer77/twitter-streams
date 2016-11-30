import java.io.File
import java.lang.Math._

import cats.data.Xor
import com.codahale.metrics.{Meter => DWMeter}

import scala.collection.concurrent.TrieMap
import scala.util.Success


object meter {
  private val INTERVAL: Int = 5
  private val SECONDS_PER_MINUTE: Double = 60.0
  private val SIXTY_MINUTES = 60
  private val M60_ALPHA: Double = 1 - exp(-INTERVAL / SECONDS_PER_MINUTE / SIXTY_MINUTES)

  class CustomMeter extends DWMeter {
    import java.util.concurrent.TimeUnit
    private val m60 = new com.codahale.metrics.EWMA(M60_ALPHA, INTERVAL, TimeUnit.SECONDS)

    override def mark(n: Long): Unit = {
      super.mark(n)
      m60.update(n)
    }

    def get60MinuteRate() = m60.getRate(TimeUnit.SECONDS)
  }

  def meter: CustomMeter = new CustomMeter
}


object Stats extends nl.grons.metrics.scala.DefaultInstrumented {
  import Utils._

  val totalMessages = metrics.counter("total-messages")
  val totalWarnings = metrics.counter("total-warnings")
  val totalSingleEvents = metrics.counter("total-single-events")
  val totalOtherEvents = metrics.counter("total-other-events")
  val totalUnknownEvents = metrics.counter("total-unknown-events")
  val totalTweetsEmojis = metrics.counter("total-tweets-with-emojis")
  val tweetsMeter = meter.meter

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

  private def top(map: TrieMap[String,Long], count: Int = 5) = {
    map.top(5).map {
      case (key, count) => s"$key: $count"
    }
  }

  private val hashTags = new TrieMap[String,Long]()
  def collectHashTags(tweet: Tweet): Unit = {
    tweet.entities.hashtags.foreach(tag => hashTags.addOrIncr(tag.text))
  }

  def top5HashTags: Seq[String] = top(hashTags)

  private val languages = new TrieMap[String,Long]()
  def collectLanguages(tweet: Tweet): Unit = {
    tweet.lang.foreach(languages.addOrIncr)
  }

  def top5Languages: Seq[String] = top(languages)

  private val countries = new TrieMap[String,Long]()
  def collectCountries(tweet: Tweet): Unit = {
    tweet.place.foreach(country => countries.addOrIncr(country.country_code))
  }

  def top5Countries: Seq[String] = top(countries)

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
