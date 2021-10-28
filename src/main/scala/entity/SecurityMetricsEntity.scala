package entity

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.stream.BoundedSourceQueue
import akka.stream.QueueOfferResult.{Dropped, Enqueued}
import model.{PercentChangeMsg, Quote}
import org.slf4j.LoggerFactory

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
          replyTo ! Ack()

          val newQuoteList = Quote.updateQuoteWindow(quote :: quoteList, 5)
          val percentChange: Double = Quote.calculatePercentChange(newQuoteList)

          if (Math.abs(percentChange) > 1) {
            log.info(s"Actor Percent Threshold Triggered: $percentChange")
            val result = source
              .offer(
                PercentChangeMsg(quote.company.symbol, percentChange)
              )

            result match {
              case Enqueued => log.info("Message Enqueued")
              case Dropped  => log.info("Message Dropped")
            }

          } else {
            log.info(s"Percent Change: $percentChange")
          }

          updated(newQuoteList)
      }
    }

    updated(List.empty[Quote])

  }
}
