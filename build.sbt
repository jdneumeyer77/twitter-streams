name := """twitter-stream"""

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"com.typesafe.akka" %% "akka-stream" % "2.4.12",
	"com.typesafe.akka" %% "akka-http-experimental" % "2.4.11",
	"com.typesafe.akka" %% "akka-http-core" % "2.4.11",
	"com.hunorkovacs" %% "koauth" % "1.1.0", // no scala 2.12 support. :(
	"org.json4s" %% "json4s-native" % "3.4.0"
)