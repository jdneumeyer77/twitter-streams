import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import com.hunorkovacs.koauth.domain.KoauthRequest
import com.hunorkovacs.koauth.service.consumer.DefaultConsumerService

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

object Auth {
  import AkkaContext._

  private val consumerKey = ""
  private val consumerSecret = ""
  private val accessToken = ""
  private val accessTokenSecret = ""

  val oauthConsumer = new DefaultConsumerService(system.dispatcher)

  def authorizationHeader(url: String)(implicit ec: ExecutionContext) = {
    val oauthHeader: Future[String] = oauthConsumer.createOauthenticatedRequest(
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

    oauthHeader.onFailure { case fail =>
      println(s"Unable to obtain oauth header! Exiting! Reason: \n $fail")
      System.exit(7)
    }

    // The program is isn't useful without this...
    val authorizationHeader = Await.result(oauthHeader, Duration(5, TimeUnit.SECONDS))

    List(HttpHeader.parse("Authorization", authorizationHeader) match {
      case ParsingResult.Ok(h, _) => Some(h)
      case _ => None
    }).flatten
  }

}
