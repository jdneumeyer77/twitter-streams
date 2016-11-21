/**
  * This is a fully working example of Twitter's Streaming API client.
  * NOTE: this may stop working if at any point Twitter does some breaking changes to this API or the JSON structure.
  */

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model._
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, FlowShape}
import akka.util.ByteString
import cats.data.Xor
import com.hunorkovacs.koauth.domain.KoauthRequest
import com.hunorkovacs.koauth.service.consumer.DefaultConsumerService
import com.typesafe.config.ConfigFactory
import de.knutwalker.akka.stream.JsonStreamParser
import io.circe._
import io.circe.generic.semiauto._
import io.circe.generic.auto._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object TwitterStreamer extends App {

  val conf = ConfigFactory.load()

  //Get your credentials from https://apps.twitter.com and replace the values below
  private val consumerKey = "cGKbjjB6ZUZc81TQJWMNtJoUd"
  private val consumerSecret = "8H938MNsvMYHM79huL8l7rijNtSwczjLypubwzS8UKtp9KTtTL"
  private val accessToken = "4447624812-5riaoBA6VGYXLUtJYSsIbnCGdsoVo3MwUi5aOHK"
  private val accessTokenSecret = "42GTIaMCF5LqRJ7eDsrdyqutAlTELWJDcaxhfcxLiowKO"
  private val newline = ByteString("\r\n")

  private val url = "https://stream.twitter.com/1.1/statuses/sample.json"
  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  private implicit val TweetDecoder: Decoder[Tweet] = deriveDecoder[Tweet]

  private val consumer = new DefaultConsumerService(system.dispatcher)

  val source = Uri(url)

  val headers = generateAuthHeader()

  val httpRequest: HttpRequest = HttpRequest(
    method = HttpMethods.GET,
    uri = source,
    headers = headers
  )

  val connectionFlow = Http().outgoingConnectionHttps(source.authority.host.address, 443)

  val totalEvents = new AtomicLong(0)
  val totalTweets = new AtomicLong(0)

  val start = System.nanoTime()

  val msgFlow = Source.single(httpRequest)
    .via(connectionFlow)
    .flatMapConcat {
      case response if response.status.isSuccess() =>
        response.entity.withoutSizeLimit().dataBytes.async
      case other =>
        println(s"Failed! $other")
        Source.empty
    }.via(Framing.delimiter(newline, Int.MaxValue).async)
     .via(passThruInjection(() => totalEvents.incrementAndGet()).async)

  //msgFlow.runForeach(_ => totalEvents.incrementAndGet())

  val classifyJsonFlow = msgFlow.via(JsonProcessing.parallelJsonClassifier(4).async)

  val x = classifyJsonFlow
    .via(JsonProcessing.parallelTweetDecoder(4).async)
    .take(1000)
    .runForeach(_ => totalTweets.incrementAndGet())

  system.scheduler.schedule(Duration.apply(10, TimeUnit.SECONDS), Duration.apply(10, TimeUnit.SECONDS)) { () =>
    println(s"Total events processed ${totalEvents.get}")
    println(s"Total tweets processed ${totalTweets.get}")
  }

  x.onComplete { done =>
    val end = System.nanoTime()
    println(s"Took: ${Duration.fromNanos(end - start).toSeconds} seconds")
    println(s"Total events processed ${totalEvents.get}")

    system.terminate()
  }

  def generateAuthHeader() = {
    val oauthHeader: Future[String] = consumer.createOauthenticatedRequest(
      KoauthRequest(
        method = "GET",
        url = url,
        authorizationHeader = None,
        body = None
      ),
      consumerKey,
      consumerSecret,
      accessToken,
      accessTokenSecret
    ).map(_.header)

    // The program is isn't useful without this...
    val authorizationHeader = Await.result(oauthHeader, Duration(5, TimeUnit.SECONDS))

    List(HttpHeader.parse("Authorization", authorizationHeader) match {
      case ParsingResult.Ok(h, _) => Some(h)
      case _ => None
    }).flatten
  }

  def passThruInjection[A](fn: () => Unit): Flow[A,A,NotUsed] = {
    Flow[A].map { passThru =>
      fn()
      passThru
    }
  }

}