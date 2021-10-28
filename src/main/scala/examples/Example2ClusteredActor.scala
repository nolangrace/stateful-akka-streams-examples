package examples

import akka.actor.ActorSystem
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.typed.{Cluster, Join}
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.BoundedSourceQueue
import akka.stream.scaladsl.{Keep, Sink, Source}
import entity.SecurityMetricsEntity
import helper.KafkaHelper
import model.PercentChangeMsg
import org.slf4j.LoggerFactory
import runner.KafkaRunner

import scala.concurrent.duration.DurationInt

object Example2ClusteredActor {
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

    standardActorIntegration()
  }

  def standardActorIntegration() = {
    val queue: BoundedSourceQueue[PercentChangeMsg] = Source
      .queue[PercentChangeMsg](10)
      .map(x => {
        log.info(s"Stream THRESHOLD ALERT! $x")
      })
      .toMat(Sink.ignore)(Keep.left)
      .run()

    sharding.init(
      Entity(SecurityMetricsEntity.TypeKey)(createBehavior =
        entityContext =>
          SecurityMetricsEntity.apply(entityContext.entityId, queue)
      )
    )

    kafkaHelper.KafkaSource
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
