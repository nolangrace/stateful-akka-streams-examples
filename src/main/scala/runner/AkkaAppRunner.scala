package runner

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}
import akka.cluster.typed.{Cluster, Join}
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{
  CommitterSettings,
  ConsumerSettings,
  ProducerSettings,
  Subscriptions
}
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.stream.BoundedSourceQueue
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import entity.SecurityMetricsEntity
import io.github.embeddedkafka.EmbeddedKafkaConfig
import model.{Quote, ThresholdAlert}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  StringDeserializer,
  StringSerializer
}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json
import runner.KafkaRunner.getClass

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object AkkaAppRunner {
  implicit val system: ActorSystem = ActorSystem("KafkaRunner")
  implicit val ec = system.dispatcher
  val config = system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings =
    ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(s"http://localhost:${KafkaRunner.kafkaPort}")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  val committerSettings = CommitterSettings(system)
  val log = LoggerFactory.getLogger(getClass)

  val cluster = Cluster(system.toTyped)
  val sharding = ClusterSharding(system.toTyped)

  def main(args: Array[String]): Unit = {
    cluster.manager ! Join(cluster.selfMember.address)
    KafkaRunner.run()

    val queue: BoundedSourceQueue[ThresholdAlert] = Source
      .queue[ThresholdAlert](10)
      .map(x => {
        log.info(s"THRESHOLD ALERT! $x")
      })
      .toMat(Sink.ignore)(Keep.left)
      .run()

    val shardRegion
        : ActorRef[ShardingEnvelope[SecurityMetricsEntity.QuoteMessage]] =
      sharding.init(
        Entity(SecurityMetricsEntity.TypeKey)(createBehavior =
          entityContext =>
            SecurityMetricsEntity.apply(entityContext.entityId, queue)
        )
      )

    Consumer
      .committableSource(consumerSettings, Subscriptions.topics("topic"))
      .map(message => {
        val quote: Quote = Json.parse(message.record.value()).as[Quote]
        (quote, message.committableOffset)
      })
      .statefulMapConcat { () =>
        var counter = 0L

        // we return the function that will be invoked for each element
        { msg =>
          counter += 1
          // we return an iterable with the single element
          (msg, counter) :: Nil
        }
      }
      .mapAsync(1)(x => {
        log.info(s"$x")
        val securityActor: EntityRef[SecurityMetricsEntity.QuoteMessage] =
          sharding
            .entityRefFor(SecurityMetricsEntity.TypeKey, x._1._1.company.symbol)

        securityActor
          .ask[SecurityMetricsEntity.Ack](ref =>
            SecurityMetricsEntity.QuoteMessage(x._1._1, ref)
          )(5.seconds)
          .map(_ => x._1._2)
      })
      .toMat(Committer.sink(committerSettings))(DrainingControl.apply)
      .run()

    // Common kinds of state in akka streams
    // - immutable state
    // - mutable state
    //    - issues with state between multiple instances of a stream
    // - one node 1 stream
    //    - mapconcat
    // - one node many stream
    //     - actor state
    //     - ask pattern
    // - many nodes many streams
    //     - actor singleton
    //     -
  }

}
