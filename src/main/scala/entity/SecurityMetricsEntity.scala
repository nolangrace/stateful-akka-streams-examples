package entity

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.stream.BoundedSourceQueue
import model.{PercentChangeMsg, Quote}
import org.slf4j.LoggerFactory

import java.time.temporal.ChronoUnit
import scala.math.Ordered.orderingToOrdered

object SecurityMetricsEntity {
  val TypeKey = EntityTypeKey[QuoteMessage]("SecurityMetrics")

  final case class QuoteMessage(quote: Quote, replyTo: ActorRef[Ack])
  final case class Ack()

  val log = LoggerFactory.getLogger(getClass)

  def apply(
      entityId: String,
      source: BoundedSourceQueue[PercentChangeMsg]
  ): Behavior[QuoteMessage] = {
    def updated(quoteList: List[Quote]): Behavior[QuoteMessage] = {
      Behaviors.receiveMessage[QuoteMessage] {
        case QuoteMessage(quote, replyTo) =>
          val ql = (quote :: quoteList)
          val sortedQuotes = ql.sortWith(_.instant <= _.instant)
          val mostRecent = sortedQuotes(0).instant
          val filteredQuotes =
            ql.filter(q => q.instant > mostRecent.minus(10, ChronoUnit.SECONDS))

          replyTo ! Ack()

          if (filteredQuotes.size > 1) {
            val min = sortedQuotes(0)
            val max = sortedQuotes(sortedQuotes.size - 1)

            val percentChange: Double = if (min.instant < max.instant) {
              ((max.last - min.last) / min.last) * 100
            } else {
              -(((max.last - min.last) / min.last) * 100)
            }

            log.info(s"Percent Change: $percentChange")
            if (Math.abs(percentChange) > 1) {
              log.info(s"Percent Threshold Triggered: $percentChange")
              source.offer(
                PercentChangeMsg(quote.company.symbol, percentChange)
              )
            }
          }

          updated(filteredQuotes)
      }
    }

    updated(List.empty[Quote])

  }
}
