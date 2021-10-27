package examples

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.sharding.external.ExternalShardAllocationStrategy
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}
import akka.cluster.sharding.typed.{ClusterShardingSettings}
import akka.cluster.typed.{Cluster, Join}
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.BoundedSourceQueue
import akka.stream.scaladsl.{Keep, Sink, Source}
import entity.SecurityMetricsEntity
import helper.KafkaHelper
import model.{PercentChangeMsg, Quote}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json
import runner.KafkaRunner

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object Example3KafkaSharding {
  implicit val system: ActorSystem = ActorSystem("KafkaRunner")
  implicit val ec = system.dispatcher

  val config = system.settings.config.getConfig("akka.kafka.consumer")

  val log = LoggerFactory.getLogger(getClass)

  val cluster = Cluster(system.toTyped)
  val sharding = ClusterSharding(system.toTyped)
  val kafkaHelper = new KafkaHelper(system)

  def main(args: Array[String]): Unit = {
    cluster.manager ! Join(cluster.selfMember.address)
    KafkaRunner.run()

    externalShardStrategy()
  }

  def externalShardStrategy() = {
    val queue: BoundedSourceQueue[PercentChangeMsg] = Source
      .queue[PercentChangeMsg](10)
      .map(x => {
        log.info(s"THRESHOLD ALERT! $x")
      })
      .toMat(Sink.ignore)(Keep.left)
      .run()

    val (kafkaSource, messageExtractor) = kafkaHelper.getKafkaShardedSourceTools

    messageExtractor.map(extractor =>
      // Setup Our Cluster Sharding for our Actor
      // Using the information Extracted from Kafka
      ClusterSharding(system.toTyped).init(
        Entity(SecurityMetricsEntity.TypeKey)(createBehavior =
          entityContext =>
            SecurityMetricsEntity.apply(entityContext.entityId, queue)
        )
          .withAllocationStrategy(
            new ExternalShardAllocationStrategy(
              system,
              SecurityMetricsEntity.TypeKey.name
            )
          )
          .withMessageExtractor(extractor)
          .withSettings(ClusterShardingSettings(system.toTyped))
      )
    )

    kafkaSource
      .map(message => {
        val quote: Quote = Json.parse(message.record.value()).as[Quote]
        (quote, message.committableOffset)
      })
      .mapAsync(1)(kafkaMessage => {
        val quote = kafkaMessage._1
        val kafkaOffset = kafkaMessage._2

        val securityActor: EntityRef[SecurityMetricsEntity.QuoteMessage] =
          sharding
            .entityRefFor(
              SecurityMetricsEntity.TypeKey,
              quote.company.symbol
            )

        securityActor
          .ask[SecurityMetricsEntity.Ack](ref =>
            SecurityMetricsEntity.QuoteMessage(quote, ref)
          )(5.seconds)
          .map(_ => kafkaOffset)
      })
      .toMat(kafkaHelper.KafkaSink)(DrainingControl.apply)
      .run()
  }

}
