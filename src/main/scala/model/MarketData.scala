package model

import com.fasterxml.jackson.annotation.JsonFormat
import play.api.libs.json.{Format, Json}

import java.time.Instant
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
}

case class Company(symbol: String, name: String)
object Company {
  implicit val format: Format[Company] = Json.format

  def randomCompany(): Company =
    companies(Random.nextInt(companies.size - 1))

  val companies = List(
    Company("TSLA", "Tesla"),
    Company("AAPL", "Apple"),
    Company("GM", "General Motors"),
    Company("GE", "General Electric"),
    Company("MSFT", "Microsoft"),
    Company("AMZN", "Amazon")
  )
}

case class ThresholdAlert(symbol: String, percentage: Double)
object ThresholdAlert {
  implicit val format: Format[ThresholdAlert] = Json.format
}
