package runner

import akka.actor.ActorSystem
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.sharding.external.ExternalShardAllocationStrategy
import akka.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}
import akka.cluster.typed.{Cluster, Join}
import akka.kafka.cluster.sharding.KafkaClusterSharding
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{
  CommitterSettings,
  ConsumerRebalanceEvent,
  ConsumerSettings,
  Subscriptions
}
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.stream.BoundedSourceQueue
import akka.stream.scaladsl.{Keep, Sink, Source}
import entity.SecurityMetricsEntity
import entity.SecurityMetricsEntity.QuoteMessage
import model.{Configs, PercentChangeMsg, Quote}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  StringDeserializer
}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

import java.time.temporal.ChronoUnit
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.math.Ordered.orderingToOrdered
import scala.util.{Failure, Success}

object AkkaAppRunner {
  implicit val system: ActorSystem = ActorSystem("KafkaRunner")
  implicit val ec = system.dispatcher

  val config = system.settings.config.getConfig("akka.kafka.consumer")

  val kafkaConsumerSettings =
    ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(Configs.kafkaBootstrapServers)
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  val committerSettings = CommitterSettings(system)
  val log = LoggerFactory.getLogger(getClass)

  val cluster = Cluster(system.toTyped)
  val sharding = ClusterSharding(system.toTyped)

  def main(args: Array[String]): Unit = {
    cluster.manager ! Join(cluster.selfMember.address)
    KafkaRunner.run()

    mapConcatStream()
//    standardActorIntegration()

//    externalShardStrategy()

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

  def mapConcatStream() = {
    val consumerSettings =
      ConsumerSettings(
        system,
        new StringDeserializer,
        new ByteArrayDeserializer
      )
        .withBootstrapServers(Configs.kafkaBootstrapServers)
        .withGroupId(
          SecurityMetricsEntity.TypeKey.name
        ) // use the same group id as we used in the `EntityTypeKey` for `User`

    Consumer
      .committableSource(
        consumerSettings,
        Subscriptions
          .topics(Configs.quoteTopic)
      )
      .map(message => {
        val quote: Quote = Json.parse(message.record.value()).as[Quote]
        (quote, message.committableOffset)
      })
      .groupBy(maxSubstreams = 10, msg => msg._1.company.symbol)
      .statefulMapConcat { () =>
        var quoteList = List.empty[Quote]

        // we return the function that will be invoked for each element
        { msg =>
          val ql = (msg._1 :: quoteList)
          val sortedQuotes = ql.sortWith(_.instant <= _.instant)
          val mostRecent = sortedQuotes(0).instant
          val filteredQuotes =
            ql.filter(q => q.instant > mostRecent.minus(10, ChronoUnit.SECONDS))

          quoteList = filteredQuotes
          if (filteredQuotes.size > 1) {
            val min = sortedQuotes(0)
            val max = sortedQuotes(sortedQuotes.size - 1)

            val percentChange: Double = if (min.instant < max.instant) {
              ((max.last - min.last) / min.last) * 100
            } else {
              -(((max.last - min.last) / min.last) * 100)
            }
            val pcm =
              PercentChangeMsg(msg._1.company.symbol, percentChange)
            (pcm, msg._2) :: Nil
          } else {
            val pcm =
              PercentChangeMsg(msg._1.company.symbol, 0.0)
            (pcm, msg._2) :: Nil
          }
        }
      }
      .mergeSubstreams // merge back into a stream
      .map(msg => {
        log.info(s"Percent Change: ${msg._1}")
        if (Math.abs(msg._1.percentage) > 1) {
          log.info(
            s"Percent Threshold Triggered: ${msg._1}"
          )
        }
        msg._2
      })
      .toMat(Committer.sink(committerSettings))(DrainingControl.apply)
      .run()

  }

  def standardActorIntegration() = {
    val queue: BoundedSourceQueue[PercentChangeMsg] = Source
      .queue[PercentChangeMsg](10)
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

    val consumerSettings =
      ConsumerSettings(
        system,
        new StringDeserializer,
        new ByteArrayDeserializer
      )
        .withBootstrapServers(Configs.kafkaBootstrapServers)
        .withGroupId(
          SecurityMetricsEntity.TypeKey.name
        ) // use the same group id as we used in the `EntityTypeKey` for `User`

    Consumer
      .committableSource(
        consumerSettings,
        Subscriptions
          .topics(Configs.quoteTopic)
      )
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
            .entityRefFor(
              SecurityMetricsEntity.TypeKey,
              x._1._1.company.symbol
            )

        securityActor
          .ask[SecurityMetricsEntity.Ack](ref =>
            SecurityMetricsEntity.QuoteMessage(x._1._1, ref)
          )(5.seconds)
          .map(_ => x._1._2)
      })
      .toMat(Committer.sink(committerSettings))(DrainingControl.apply)
      .run()

  }

  def externalShardStrategy() = {
    val queue: BoundedSourceQueue[PercentChangeMsg] = Source
      .queue[PercentChangeMsg](10)
      .map(x => {
        log.info(s"THRESHOLD ALERT! $x")
      })
      .toMat(Sink.ignore)(Keep.left)
      .run()

    // automatically retrieving the number of partitions requires a round trip to a Kafka broker
    val messageExtractor: Future[
      KafkaClusterSharding.KafkaShardingNoEnvelopeExtractor[QuoteMessage]
    ] =
      KafkaClusterSharding(system).messageExtractorNoEnvelope(
        timeout = 10.seconds,
        topic = Configs.quoteTopic,
        entityIdExtractor = (msg: QuoteMessage) => msg.quote.company.symbol,
        settings = kafkaConsumerSettings
      )

    messageExtractor.onComplete {
      case Success(extractor) =>
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
      case Failure(ex) =>
        system.log.error(
          "An error occurred while obtaining the message extractor",
          ex
        )
    }

    // obtain an Akka classic ActorRef that will handle consumer group rebalance events
    val rebalanceListener: akka.actor.typed.ActorRef[ConsumerRebalanceEvent] =
      KafkaClusterSharding(system).rebalanceListener(
        SecurityMetricsEntity.TypeKey
      )

    // convert the rebalance listener to a classic ActorRef until Alpakka Kafka supports Akka Typed
    import akka.actor.typed.scaladsl.adapter._
    val rebalanceListenerClassic: akka.actor.ActorRef =
      rebalanceListener.toClassic

    val consumerSettings =
      ConsumerSettings(
        system,
        new StringDeserializer,
        new ByteArrayDeserializer
      )
        .withBootstrapServers(Configs.kafkaBootstrapServers)
        .withGroupId(
          SecurityMetricsEntity.TypeKey.name
        ) // use the same group id as we used in the `EntityTypeKey` for `User`

    // pass the rebalance listener to the topic subscription
    val subscription = Subscriptions
      .topics(Configs.quoteTopic)
      .withRebalanceListener(rebalanceListenerClassic)

    Consumer
      .committableSource(consumerSettings, subscription)
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
            .entityRefFor(
              SecurityMetricsEntity.TypeKey,
              x._1._1.company.symbol
            )

        securityActor
          .ask[SecurityMetricsEntity.Ack](ref =>
            SecurityMetricsEntity.QuoteMessage(x._1._1, ref)
          )(5.seconds)
          .map(_ => x._1._2)
      })
      .toMat(Committer.sink(committerSettings))(DrainingControl.apply)
      .run()
  }

}
