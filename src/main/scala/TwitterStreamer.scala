import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object AkkaContext {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
}

object TwitterStreamer extends App {
  import AkkaContext._
  import Flows._
  import Stats._

  private val emojiFile = "emoji.json"
  private val url = "https://stream.twitter.com/1.1/statuses/sample.json"
  private val source = Uri(url)

  val emojData = Stats.Emojis.readData(emojiFile)

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

  // stats collection
  broadcastTweets.runForeach(Stats.Hashtags.collect)
  broadcastTweets.runForeach(Stats.Languages.collect)
  broadcastTweets.runForeach(Stats.Countries.collect)
  broadcastTweets.runForeach(Stats.Urls.collect)
  broadcastTweets.runForeach(Stats.Photos.collect)
  if(emojData.nonEmpty) broadcastTweets.runForeach(Stats.Emojis.collect(emojData))

  system.scheduler.schedule(1.minutes, 3.minutes) {
    displayStats()
  }

  Console.in.readLine()

  stopStream()

  displayTotalStats()
  displayStats()

  system.terminate()

  def displayTotalStats(): Unit = {
    val end = System.nanoTime()
    println("=" * 20)
    println("Finish stats:")
    println(s"Runtime: ${Duration.fromNanos(end - start).toMinutes} minutes")
    println(s"Total messages processed ${totalMessages.count}")
    println(s"Total warnings ${totalWarnings.count}")
    println(s"Total other events ${totalOtherEvents.count}")
    println(s"Total single field events ${totalSingleEvents.count}")
    println(s"Total unknown events ${totalUnknownEvents.count}")
    println(s"Total standard tweets events ${tweetsMeter.count}")
  }

  def displayStats(): Unit = {
    println(s"Tweets per second: ${tweetsMeter.meanRate}")
    println(s"Average tweets per second in the 1 minute: ${tweetsMeter.oneMinuteRate()}")
    println(s"Average tweets per second in the last hour: ${tweetsMeter.sixtyMinuteRate()}")

    println(s"Percent of tweets with emojis (${totalTweetsEmojis.count}): ${percentOfTweets(totalTweetsEmojis.count)}")
    println(s"Percent of tweets with urls (${Stats.Urls.total.count}): ${percentOfTweets(Stats.Urls.total.count)}")
    println(s"Percent of tweets with photos (${Stats.Photos.total.count}): ${percentOfTweets(Stats.Photos.total.count)}")

    println(s"Top Hashtags: ${Stats.Hashtags.top().mkString(", ")}")
    println(s"Top languages: ${Stats.Languages.top().mkString(", ")}")
    println(s"Top countries: ${Stats.Countries.top().mkString(", ")}")
    println(s"Top emojis: ${Stats.Emojis.top().mkString(", ")}")
    println(s"Top urls: ${Stats.Urls.top().mkString(", ")}")
  }

}