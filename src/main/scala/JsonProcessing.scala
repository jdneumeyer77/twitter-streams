import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Merge}
import akka.util.ByteString
import cats.data.Xor
import de.knutwalker.akka.stream.JsonStreamParser
import io.circe.{Decoder, Json, JsonObject}
import GraphDSL.Implicits._

sealed trait StreamedTweetType
case class TweetEvent(json: Json) extends StreamedTweetType
case class WarningEvent(json: Json) extends StreamedTweetType
case class Event(json: Json) extends StreamedTweetType
case class SingleFieldEvent(json: Json) extends StreamedTweetType
case class UnknownEvent(json: Json) extends StreamedTweetType

object JsonProcessing extends JsonProcessing

trait JsonProcessing {

  // decode json to standard tweets.
  def parallelTweetDecoder(parallel: Int)(implicit decoder: Decoder[Tweet]): Flow[StreamedTweetType, Tweet, NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      val dispatcher = builder.add(Balance[StreamedTweetType](parallel))
      val merged = builder.add(Merge[Tweet](parallel))

      (0 until parallel).foreach { number =>
        dispatcher.out(number) ~> decodeTweet.async ~> merged.in(number)
      }

      FlowShape(dispatcher.in, merged.out)
    })

  // parse and classify the json.
  def parallelJsonClassifier(parallel: Int): Flow[ByteString, StreamedTweetType, NotUsed] = {
    import io.circe.jawn.CirceSupportParser._
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      val dispatcher = builder.add(Balance[ByteString](parallel))
      val merged = builder.add(Merge[StreamedTweetType](parallel))

      (0 until parallel).foreach { number =>
        dispatcher.out(number) ~> JsonStreamParser.flow[Json].async ~>
          Flow[Json].map(tweetClassifier).async ~> merged.in(number)
      }

      FlowShape(dispatcher.in, merged.out)
    })
  }

  private def decodeTweet(implicit decoder: Decoder[Tweet]): Flow[StreamedTweetType, Tweet, NotUsed] = {
    Flow[StreamedTweetType].collect {
      case TweetEvent(json) => decoder.decodeJson(json)
    }.collect {
      case Xor.Right(tweet) => tweet
    }
  }

  private def tweetClassifier(json: Json): StreamedTweetType = {
    val obj = json.asObject // top level object

    if(obj.exists(jsonHasFields("warning"))) WarningEvent(json)
    else if(obj.exists(jsonHasFields("event"))) Event(json)
    else if(obj.exists(_.size == 1)) SingleFieldEvent(json)
    else if(obj.exists(jsonHasFields("id_str", "text", "entities"))) TweetEvent(json) // likely...
    else UnknownEvent(json)

  }

  def jsonHasFields(fields: String*)(json: JsonObject): Boolean = {
    fields.forall(json.contains)
  }
}
