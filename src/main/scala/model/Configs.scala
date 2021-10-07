package model

object Configs {
  val kafkaPort = 12346
  val kafkaBootstrapServers = s"http://localhost:${kafkaPort}"
  val quoteTopic = "quote-topic"
}
