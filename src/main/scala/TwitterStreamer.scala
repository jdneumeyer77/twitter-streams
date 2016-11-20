/**
 * This is a fully working example of Twitter's Streaming API client.
 * NOTE: this may stop working if at any point Twitter does some breaking changes to this API or the JSON structure.
 */

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Framing, Sink, Source}
import akka.util.ByteString
import cats.data.Xor
import com.hunorkovacs.koauth.domain.KoauthRequest
import com.hunorkovacs.koauth.service.consumer.DefaultConsumerService
import com.typesafe.config.ConfigFactory
import de.knutwalker.akka.stream.JsonStreamParser

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto._

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

	private val consumer = new DefaultConsumerService(system.dispatcher)

  implicit val TweetDecoder: Decoder[Tweet] = deriveDecoder[Tweet]

	val source = Uri(url)

	val headers = generateAuthHeader()

	val httpRequest: HttpRequest = HttpRequest(
		method = HttpMethods.GET,
		uri = source,
		headers = headers
	)

	val connectionFlow = Http().outgoingConnectionHttps(source.authority.host.address, 443)

  val newline = ByteString("\r\n")

	val x = Source.single(httpRequest)
				.via(connectionFlow)
				.flatMapConcat {
					case response if response.status.isSuccess() =>
						response.entity.withoutSizeLimit().dataBytes.async
					case other =>
						println(s"Failed! $other")
						Source.empty
				}.async
         .via(Framing.delimiter(newline, Int.MaxValue).async)
         .via(decode[Tweet].async)
         .to(Sink.foreach(x => println(x.id_str)))//.onComplete(_ => system.terminate)

  x.run()
  // like de.knutwalker.akka.stream.support.CirceStreamSupport.decode[T]
  // but ignore unparsable tweets.
  def decode[A: Decoder]: Flow[ByteString, A, NotUsed] = {
    import io.circe.jawn.CirceSupportParser._

    JsonStreamParser.flow[Json].filter(jsonHasFields("created_at", "text", "id_str")(_))
      .map(json => decodeJson[A](json))
      .collect {
        case Xor.Right(e) => e
      }
  }

  def decodeJson[A](json: Json)(implicit decoder: Decoder[A]) = {
    decoder.decodeJson(json)
  }

  def jsonHasFields(fields: String*)(json: Json): Boolean = {
    val obj = json.cursor.focus.asObject
    obj.exists(current => current.size >= fields.size && fields.forall(current(_).isDefined))
  }

  def jsonHasAtleastOneField(fields: String*)(json: Json): Boolean = {
    val cursor = json.cursor
    fields.exists(cursor.downField(_).isDefined)
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
}