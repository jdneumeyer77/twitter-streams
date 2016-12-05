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

  val headers = Auth.authorizationHeader(url)

  val httpRequest: HttpRequest = HttpRequest(
    method = HttpMethods.GET,
    uri = source,
    headers = headers
  )

  // TODO: Nice to split 4-ways with classifier.
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

  // TODO: Switch to Sink.tick (and produce a stats snapshot).
  // Then it can be push to a DB or whatever periodically.
  system.scheduler.schedule(1.minutes, 3.minutes) {
    Reporter.displayStats(true)
  }

  println("Press enter to stop application, and display final statistics.")
  Console.in.readLine()

  stopStream()

  Reporter.displayFinalStats()
  Reporter.displayStats(false)

  system.terminate()
}