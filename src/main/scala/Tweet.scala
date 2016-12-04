import io.circe.Decoder
import io.circe.generic.semiauto._

object Tweet {
	implicit val UrlsDecoder: Decoder[Url] = deriveDecoder
	implicit val MediaDecoder: Decoder[Media] = deriveDecoder
	implicit val HashTagDecoder: Decoder[Hashtag] = deriveDecoder
	implicit val EntitiesDecoder: Decoder[Entities] = deriveDecoder
	implicit val PlaceDecoder: Decoder[Place] = deriveDecoder
	implicit val TweetDecoder: Decoder[Tweet] = deriveDecoder
}

case class Tweet(
	created_at: String,
	entities: Entities,
	favorite_count: Option[Int],
	filter_level: String,
	id_str: String,
	lang: Option[String],
	place: Option[Place],
	retweet_count: Int,
	text: String
)

case class Place(country_code: String)

case class Entities(hashtags: Seq[Hashtag], urls: Seq[Url], media: Option[Seq[Media]])
case class Media(expanded_url: String)
case class Hashtag(text: String)
case class Url(expanded_url: String)