package examples

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.typed.{Cluster, Join}
import akka.kafka.scaladsl.Consumer.DrainingControl
import helper.KafkaHelper
import model.{PercentChangeMsg, Quote}
import org.slf4j.LoggerFactory
import runner.KafkaRunner

object Example1MapConcat {
  implicit val system: ActorSystem = ActorSystem("KafkaRunner")
  implicit val ec = system.dispatcher

  val log = LoggerFactory.getLogger(getClass)

  val cluster = Cluster(system.toTyped)
  val sharding = ClusterSharding(system.toTyped)
  val kafkaHelper = new KafkaHelper(system)

  def main(args: Array[String]): Unit = {
    cluster.manager ! Join(cluster.selfMember.address)
    KafkaRunner.run()

    mapConcatStream()
  }

  def mapConcatStream() = {
    kafkaHelper.KafkaSource
      .groupBy(maxSubstreams = 10, msg => msg._1.company.symbol)
      .statefulMapConcat { () =>
        var quoteList = List.empty[Quote]

        // we return the function that will be invoked for each element
        { kafkaMessage =>
          val quote = kafkaMessage._1
          val kafkaOffset = kafkaMessage._2

          quoteList = Quote.updateQuoteWindow(quote :: quoteList, 10)
          val percentChange: Double = Quote.calculatePercentChange(quoteList)
          val pcm = PercentChangeMsg(quote.company.symbol, percentChange)
          (pcm, kafkaOffset) :: Nil
        }
      }
      .mergeSubstreams // merge back into a stream
      .map(msg => {
        log.info(s"Percent Change: ${msg._1}")
        if (Math.abs(msg._1.percentage) > 1) {
          log.info(
            s"THRESHOLD ALERT!: ${msg._1}"
          )
        }
        msg._2
      })
      .toMat(kafkaHelper.KafkaSink)(DrainingControl.apply)
      .run()

  }
}
