import java.io.{File, InputStreamReader}
import java.nio.file.{Files, Path}

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import cats.data.Xor

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Success, Try}

object AkkaContext {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
}

object TwitterStreamer extends App {
  import Stats._
  import Flows._
  import AkkaContext._

  private val emojiFile = "emoji.json"
  private val url = "https://stream.twitter.com/1.1/statuses/sample.json"
  private val source = Uri(url)

  val emojData = readEmojiData(emojiFile)

  println(s"emoji data dictionary size: ${emojData.size}")

  val headers = Auth.authorizationHeader(url)

  val httpRequest: HttpRequest = HttpRequest(
    method = HttpMethods.GET,
    uri = source,
    headers = headers
  )

  val tweetsFlow = msgFlow(httpRequest)
                .via(classifyTweetsFlow)
                .alsoTo(Sink.foreach(countTypes))
                .via(produceTweetsFlow)

  val broadcastTweets = tweetsFlow.toMat(BroadcastHub.sink)(Keep.right).run()

  broadcastTweets.runForeach(Stats.collectHashTags)
  broadcastTweets.runForeach(Stats.collectLanguages)
  broadcastTweets.runForeach(Stats.collectCountries)

  if(emojData.nonEmpty) broadcastTweets.runForeach(Stats.collectEmojis(emojData))

  system.scheduler.schedule(1 minutes, 3 minutes) {
    displayStats()
  }

  Console.in.readLine()

  stopStream()

  displayTotalStats()
  displayStats()

  system.terminate()

  def displayTotalStats(): Unit = {
    val end = System.nanoTime()
    println("Finish stats:")
    println(s"Runtime: ${Duration.fromNanos(end - start).toSeconds} seconds")
    println(s"Total messages processed ${totalMessages.count}")
    println(s"Total warnings ${totalWarnings.count}")
    println(s"Total other events ${totalOtherEvents.count}")
    println(s"Total single field events ${totalSingleEvents.count}")
    println(s"Total unknown events ${totalUnknownEvents.count}")
    println(s"Total standard tweets events ${tweetsMeter.getCount}")
  }

  def displayStats(): Unit = {
    println(s"Tweets per second: ${tweetsMeter.getMeanRate}")
    println(s"Average tweets per second in the 1 minute: ${tweetsMeter.getOneMinuteRate()}")
    println(s"Average tweets per second in the 15 minutes: ${tweetsMeter.getFifteenMinuteRate()}")
    println(s"Average tweets per second in the last hour: ${tweetsMeter.getSixtyMinuteRate()}")

    println(s"Percent of tweets with emojis (${totalTweetsEmojis.count}): ${percentOfTweets(totalTweetsEmojis.count)}")
    println(s"Percent of tweets with urls (${totalTweetsUrls.count}): ${percentOfTweets(totalTweetsUrls.count)}")
    println(s"Percent of tweets with photos (${totalTweetsPhotos.count}): ${percentOfTweets(totalTweetsPhotos.count)}")

    println(s"Top Hashtags: ${Stats.top5HashTags.mkString(", ")}")
    println(s"Top languages: ${Stats.top5Languages.mkString(", ")}")
    println(s"Top countries: ${Stats.top5Countries.mkString(", ")}")
    println(s"Top emojis: ${Stats.top5Emojis.mkString(", ")}")
    println(s"Top urls: ${Stats.top5Urls.mkString(", ")}")
  }

}