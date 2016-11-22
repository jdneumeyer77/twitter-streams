import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import com.hunorkovacs.koauth.domain.KoauthRequest
import com.hunorkovacs.koauth.service.consumer.DefaultConsumerService

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

object Auth {
  private val consumerKey = "cGKbjjB6ZUZc81TQJWMNtJoUd"
  private val consumerSecret = "8H938MNsvMYHM79huL8l7rijNtSwczjLypubwzS8UKtp9KTtTL"
  private val accessToken = "4447624812-5riaoBA6VGYXLUtJYSsIbnCGdsoVo3MwUi5aOHK"
  private val accessTokenSecret = "42GTIaMCF5LqRJ7eDsrdyqutAlTELWJDcaxhfcxLiowKO"

  def authorizationHeader(oauthConsumer: DefaultConsumerService, url: String)(implicit ec: ExecutionContext) = {
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
      println(s"Unable to obtain oauth header! Reason: \n $fail")
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
