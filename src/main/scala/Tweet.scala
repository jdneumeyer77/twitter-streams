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

case class Place(
	country_code: String,
	full_name: Option[String],
	name: Option[String]
)

case class Entities(hashtags: Seq[Hashtag],
										urls: Seq[Url])
case class Hashtag(text: String)
case class Url(expanded_url: Option[String])