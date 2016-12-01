import Stats._
import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Flow, Framing, Keep, Source}
import akka.util.ByteString

object Flows {
  import AkkaContext._
  import Tweet._
  private val newline = ByteString("\r\n")

  def connectionFlow(uri: Uri) = Http().outgoingConnectionHttps(uri.authority.host.address, 443)

  private val killSwitch = KillSwitches.shared("my-kill-switch")
  def stopStream() = killSwitch.shutdown()

  val classifyTweetsFlow = Flow[ByteString].via(JsonProcessing.parallelJsonClassifier(4).async)

  val produceTweetsFlow = Flow[StreamedTweetType].via(JsonProcessing.parallelTweetDecoder(4).async)

  def msgFlow(httpRequest: HttpRequest) =
    Source.single(httpRequest)
      .via(connectionFlow(httpRequest.uri))
      .flatMapConcat {
        case response if response.status.isSuccess() =>
          response.entity.withoutSizeLimit().dataBytes
        case other =>
          println(s"Failed! $other")
          Source.empty
      }.via(killSwitch.flow)
      .via(Framing.delimiter(newline, Int.MaxValue).async)
      .filter(_.nonEmpty)
      .via(passThruInjection(() => totalMessages.inc()))
      .async

  def passThruInjection[A](fn: () => Unit): Flow[A,A,NotUsed] = {
    Flow[A].map { passThru =>
      fn()
      passThru
    }
  }
}
