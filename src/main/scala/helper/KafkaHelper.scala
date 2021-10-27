package helper

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter.TypedActorRefOps
import akka.kafka.cluster.sharding.KafkaClusterSharding
import akka.kafka.{
  CommitterSettings,
  ConsumerRebalanceEvent,
  ConsumerSettings,
  Subscriptions
}
import akka.kafka.scaladsl.{Committer, Consumer}
import entity.SecurityMetricsEntity
import entity.SecurityMetricsEntity.QuoteMessage
import model.Quote
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  StringDeserializer
}
import play.api.libs.json.Json

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class KafkaHelper(system: ActorSystem) {
  private val kafkaPort = 12346
  private val kafkaBootstrapServers = s"http://localhost:${kafkaPort}"
  private val quoteTopic = "quote-topic"

  val config = system.settings.config.getConfig("akka.kafka.consumer")

  private val kafkaConsumerSettings =
    ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(kafkaBootstrapServers)
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private val subscription = Subscriptions
    .topics(quoteTopic)

  private val committerSettings = CommitterSettings(system)

  val KafkaSource = {
    Consumer
      .committableSource(kafkaConsumerSettings, subscription)
      .map(message => {
        val quote: Quote = Json.parse(message.record.value()).as[Quote]
        (quote, message.committableOffset)
      })
  }

  val KafkaSink = {
    Committer.sink(committerSettings)
  }

  // https://doc.akka.io/docs/alpakka-kafka/current/cluster-sharding.html
  val getKafkaShardedSourceTools = {

    // automatically retrieving the number of partitions requires a round trip to a Kafka broker
    val messageExtractor: Future[
      KafkaClusterSharding.KafkaShardingNoEnvelopeExtractor[QuoteMessage]
    ] =
      KafkaClusterSharding(system).messageExtractorNoEnvelope(
        timeout = 10.seconds,
        topic = quoteTopic,
        entityIdExtractor = (msg: QuoteMessage) => msg.quote.company.symbol,
        settings = kafkaConsumerSettings
      )

    /*
       The Rebalance Listener is a pre-defined Actor that will handle ConsumerRebalanceEvents that will
       update the Akka Cluster External Sharding strategy when subscribed partitions are re-assigned to consumers
       running on different cluster nodes.
     */
    val rebalanceListener =
      KafkaClusterSharding(system)
        .rebalanceListener(
          SecurityMetricsEntity.TypeKey
        )
        .toClassic

    val kafkaShardingConsumerSettings =
      ConsumerSettings(
        system,
        new StringDeserializer,
        new ByteArrayDeserializer
      )
        .withBootstrapServers(kafkaBootstrapServers)
        .withGroupId(
          SecurityMetricsEntity.TypeKey.name
        ) // use the same group id as we used in the `EntityTypeKey` for `User`

    val shardingSubscription = Subscriptions
      .topics(quoteTopic)
      .withRebalanceListener(rebalanceListener)

    val consumer = Consumer
      .committableSource(kafkaShardingConsumerSettings, shardingSubscription)

    (consumer, messageExtractor)
  }

}
