name := "stateful-akka-streams"

version := "0.1"

scalaVersion := "2.13.6"

val AkkaVersion = "2.6.16"
val JacksonVersion = "2.11.4"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion,
  "io.github.embeddedkafka" %% "embedded-kafka" % "2.8.1",
  "com.typesafe.akka" %% "akka-stream-kafka" % "2.1.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % JacksonVersion,
  "com.typesafe.play" %% "play-json" % "2.9.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)
