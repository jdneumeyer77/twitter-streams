import java.io.File
import java.lang.Math._
import java.net.URL
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicLong, LongAdder}

import cats.data.Xor
import com.codahale.metrics.{Clock, EWMA}

import scala.collection.concurrent.TrieMap
import scala.util.Success


object meter {
  // Needed for EWMA.
  private val INTERVAL: Int = 5
  private val SECONDS_PER_MINUTE: Double = 60.0
  private val SIXTY_MINUTES = 60
  private val M60_ALPHA: Double = 1 - exp(-INTERVAL / SECONDS_PER_MINUTE / SIXTY_MINUTES)
  private val TICK_INTERVAL: Long = TimeUnit.SECONDS.toNanos(INTERVAL)

  // Based on Dropwizard's meter, but it wasn't fit to be extended...
  class CustomMeter {
    import java.util.concurrent.TimeUnit
    private val m1Rate: EWMA = EWMA.oneMinuteEWMA
    private val m60Rate = new com.codahale.metrics.EWMA(M60_ALPHA, INTERVAL, TimeUnit.SECONDS)

    private val counter = new LongAdder()
    private val clock = Clock.defaultClock()
    private val startTime = clock.getTick
    private val lastTick = new AtomicLong(startTime)

    def mark(): Unit = {
      mark(1)
    }

    def mark(n: Long): Unit = {
      tickIfNecessary()
      counter.add(n)
      m1Rate.update(n)
      m60Rate.update(n)
    }

    private def tickIfNecessary() {
      val oldTick = lastTick.get
      val newTick = clock.getTick
      val age = newTick - oldTick
      if (age > TICK_INTERVAL) {
        val newIntervalStartTick = newTick - age % TICK_INTERVAL
        if (lastTick.compareAndSet(oldTick, newIntervalStartTick)) {
          val requiredTicks: Long = age / TICK_INTERVAL
          (0L until requiredTicks).foreach { _ =>
            m1Rate.tick()
            m60Rate.tick()
          }
        }
      }
    }

    def sixtyMinuteRate() = {
      tickIfNecessary()
      m60Rate.getRate(TimeUnit.SECONDS)
    }

    def oneMinuteRate() = {
      tickIfNecessary()
      m1Rate.getRate(TimeUnit.SECONDS)
    }

    def count = counter.sum()

    def meanRate = if (count == 0) 0.0
    else {
      val elapsed: Double = clock.getTick - startTime
      count / elapsed * TimeUnit.SECONDS.toNanos(1)
    }
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
  val totalTweetsUrls = metrics.counter("total-tweets-with-a-url")
  val totalTweetsPhotos = metrics.counter("total-tweets-with-a-photo")
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
  private val photoDomains = Vector("pic.twitter", "instagram", "imgur")
  def collectPhotoUrls(tweet: Tweet): Unit = {
    val containsPhotoUrl_? = tweet.entities.media.exists { media =>
      media.exists(m => photoDomains.contains(m.display_url))
    }

    if(containsPhotoUrl_?) totalTweetsPhotos.inc()
  }

  //// urls
  private val urlHostnames = new TrieMap[String,Long]()
  def collectUrls(tweet: Tweet): Unit = {
    if(tweet.entities.urls.nonEmpty) {
      totalTweetsUrls.inc()
      tweet.entities.urls.collect {
        case Url(Some(url)) =>
          new URL(url).getHost
      }.foreach(urlHostnames.addOrIncr)
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
