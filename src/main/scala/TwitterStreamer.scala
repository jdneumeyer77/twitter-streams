/**
 * This is a fully working example of Twitter's Streaming API client.
 * NOTE: this may stop working if at any point Twitter does some breaking changes to this API or the JSON structure.
 */

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.hunorkovacs.koauth.domain.KoauthRequest
import com.hunorkovacs.koauth.service.consumer.DefaultConsumerService
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.parallel.immutable
import scala.concurrent.duration.Duration

object TwitterStreamer extends App {

	val conf = ConfigFactory.load()

	//Get your credentials from https://apps.twitter.com and replace the values below
	private val consumerKey = "cGKbjjB6ZUZc81TQJWMNtJoUd"
	private val consumerSecret = "8H938MNsvMYHM79huL8l7rijNtSwczjLypubwzS8UKtp9KTtTL"
	private val accessToken = "4447624812-5riaoBA6VGYXLUtJYSsIbnCGdsoVo3MwUi5aOHK"
	private val accessTokenSecret = "42GTIaMCF5LqRJ7eDsrdyqutAlTELWJDcaxhfcxLiowKO"
	private val url = "https://stream.twitter.com/1.1/statuses/sample.json"

	implicit val system = ActorSystem()
	implicit val materializer = ActorMaterializer()
	implicit val formats = DefaultFormats

	private val consumer = new DefaultConsumerService(system.dispatcher)

	val source = Uri(url)

	val headers = generateAuthHeader()

	val httpRequest: HttpRequest = HttpRequest(
		method = HttpMethods.GET,
		uri = source,
		headers = headers
	)

	val connectionFlow = Http().outgoingConnectionHttps(source.authority.host.address, 443)

	Source.single(httpRequest)
				.via(connectionFlow)
				.flatMapConcat {
					case response if response.status.isSuccess() =>
						response.entity.dataBytes
					case other =>
						println(s"Failed! $other")
						Source.empty
				}.filter(_.startsWith("\r\n")).map {
					_.utf8String
				}.runForeach(println).onComplete(_ => system.terminate)

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
}