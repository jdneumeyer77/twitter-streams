import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString
import com.hunorkovacs.koauth.service.consumer.DefaultConsumerService
import io.circe._
import io.circe.generic.semiauto._
import io.circe.generic.auto._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object TwitterStreamer extends App {

  private val url = "https://stream.twitter.com/1.1/statuses/sample.json"
  private val source = Uri(url)

  private val newline = ByteString("\r\n")

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()
  private val oauthConsumer = new DefaultConsumerService(system.dispatcher)

  private implicit val TweetDecoder: Decoder[Tweet] = deriveDecoder[Tweet]

  val connectionFlow = Http().outgoingConnectionHttps(source.authority.host.address, 443)

  val classifyTweetsFlow = Flow[ByteString].via(JsonProcessing.parallelJsonClassifier(4).async)

  val produceTweetsFlow = Flow[StreamedTweetType].via(JsonProcessing.parallelTweetDecoder(4).async)

  val headers = Auth.authorizationHeader(oauthConsumer, url)

  val httpRequest: HttpRequest = HttpRequest(
    method = HttpMethods.GET,
    uri = source,
    headers = headers
  )

  def countTypes(tweetType: StreamedTweetType): Unit = {
    tweetType match {
      case WarningEvent(_) => totalWarnings.incrementAndGet()
      case SingleFieldEvent(_) => totalSingleEvents.incrementAndGet()
      case Event(_) => totalOtherEvents.incrementAndGet()
      case UnknownEvent(x) =>
        println(x)
        totalUnknownEvents.incrementAndGet()
      case TweetEvent(_) => totalStandardTweets.incrementAndGet()
    }
  }

  val tweetsFlow = msgFlow(httpRequest)
                .via(classifyTweetsFlow)
                .alsoTo(Sink.foreach(countTypes))
                .via(produceTweetsFlow)

  val broadcastTweets = tweetsFlow.toMat(BroadcastHub.sink)(Keep.right).run()

  broadcastTweets.take(5000).runForeach(_ => ()).onComplete { done =>
    val end = System.nanoTime()
    println(s"Took: ${Duration.fromNanos(end - start).toSeconds} seconds")
    println(s"Total messages processed ${totalMessages.get}")
    println(s"Total warnings ${totalWarnings.get}")
    println(s"Total other events ${totalOtherEvents.get}")
    println(s"Total single field events ${totalSingleEvents.get}")
    println(s"Total unknown events ${totalUnknownEvents.get}")
    println(s"Total standard tweets events ${totalStandardTweets.get}")
    println(s"Couldn't decode standard tweets events ${totalStandardTweets.get - 5000}")

    system.terminate()
  }

//  broadcastTweets.take(5000).runForeach(_ => ()).onComplete { done =>
//    val end = System.nanoTime()
//    println(s"Took: ${Duration.fromNanos(end - start).toSeconds} seconds")
//    println(s"Total events processed ${totalMessages.get}")
//  }

  val totalMessages = new AtomicLong(0)
  val totalWarnings = new AtomicLong(0)
  val totalSingleEvents = new AtomicLong(0)
  val totalOtherEvents = new AtomicLong(0)
  val totalUnknownEvents = new AtomicLong(0)
  val totalStandardTweets = new AtomicLong(0)

  val start = System.nanoTime()

  def msgFlow(httpRequest: HttpRequest) =
    Source.single(httpRequest)
    .via(connectionFlow)
    .flatMapConcat {
      case response if response.status.isSuccess() =>
        response.entity.withoutSizeLimit().dataBytes
      case other =>
        println(s"Failed! $other")
        Source.empty
    }.via(Framing.delimiter(newline, Int.MaxValue))
     .filter(_.nonEmpty)
     .via(passThruInjection(() => totalMessages.incrementAndGet()))
     .async

  def passThruInjection[A](fn: () => Unit): Flow[A,A,NotUsed] = {
    Flow[A].map { passThru =>
      fn()
      passThru
    }
  }
}