name := """twitter-stream"""

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"com.typesafe.akka" %% "akka-stream" % "2.4.14",
	"com.typesafe.akka" %% "akka-http" % "10.0.0",
	"com.typesafe.akka" %% "akka-http-core" % "10.0.0",
	"com.hunorkovacs" %% "koauth" % "1.1.0", // no scala 2.12 support. :(
	"de.knutwalker" %% "akka-stream-circe" % "3.1.0",
	"de.knutwalker" %% "akka-http-circe" % "3.1.0",
	"io.circe" %% "circe-generic" % "0.5.0",
  "nl.grons" %% "metrics-scala" % "3.5.5_a2.3"

)