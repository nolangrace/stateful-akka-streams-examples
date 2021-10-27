package model

import com.fasterxml.jackson.annotation.JsonFormat
import play.api.libs.json.{Format, Json}

import java.time.Instant
import java.time.temporal.{ChronoUnit, TemporalAmount, TemporalUnit}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.math.Ordered.orderingToOrdered
import scala.util.Random

case class Quote(
    id: Long,
    company: Company,
    bid: Long,
    ask: Long,
    last: Long,
    instant: Instant = Instant.now()
)
object Quote {
  implicit val format: Format[Quote] = Json.format

  def generateMarketData(id: Long): Quote = {
    val company = Company.randomCompany()
    val bid: Long = Random.between(1, 15)
    val spread: Long = Random.between(1, 5)
    val ask: Long = bid + spread
    val last: Long = Random.between(bid, ask)
    Quote(id, company, bid, ask, last)
  }

  def updateQuoteWindow(
      quotes: List[Quote],
      windowDurationSeconds: Long
  ): List[Quote] = {
    val sortedQuotes = quotes.sortWith(_.instant <= _.instant)
    val mostRecent = sortedQuotes(sortedQuotes.size - 1).instant
    sortedQuotes.filter(q => {
      q.instant > mostRecent
        .minus(windowDurationSeconds, ChronoUnit.SECONDS)
    })
  }

  def calculatePercentChange(quotes: List[Quote]): Double = {
    if (quotes.size == 0)
      return 0.0
    else {
      def min(q1: Quote, q2: Quote): Quote = if (q1.last < q2.last) q1 else q2

      val minQuote = quotes.reduceLeft(min)

      def max(q1: Quote, q2: Quote): Quote = if (q1.last > q2.last) q1 else q2

      val maxQuote = quotes.reduceLeft(max)

      if (minQuote.instant < maxQuote.instant) {
        ((maxQuote.last - minQuote.last) / minQuote.last) * 100
      } else {
        -(((maxQuote.last - minQuote.last) / minQuote.last) * 100)
      }
    }
  }
}

case class Company(symbol: String, name: String)
object Company {
  implicit val format: Format[Company] = Json.format

  def randomCompany(): Company =
    companies(Random.nextInt(companies.size))

  val companies = List(
    Company("TSLA", "Tesla"),
    Company("AAPL", "Apple"),
    Company("GM", "General Motors"),
    Company("GE", "General Electric"),
    Company("MSFT", "Microsoft"),
    Company("AMZN", "Amazon")
  )
}

case class PercentChangeMsg(symbol: String, percentage: Double)
object PercentChangeMsg {
  implicit val format: Format[PercentChangeMsg] = Json.format
}
