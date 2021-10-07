package runner

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.stream.scaladsl.Source
import akka.kafka.scaladsl.Producer
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import model.{Configs, Quote}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object KafkaRunner {
  implicit val system: ActorSystem = ActorSystem("KafkaRunner")
  implicit val ec = system.dispatcher

  val quoteTopic = "quote-topic"
  implicit val blaConfig = EmbeddedKafkaConfig(kafkaPort = Configs.kafkaPort)
  val config = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings =
    ProducerSettings(config, new StringSerializer, new StringSerializer)
      .withBootstrapServers(Configs.kafkaBootstrapServers)
  val log = LoggerFactory.getLogger(getClass)

  def run(): Future[Done] = {
    EmbeddedKafka.start()

    Source
      .repeat("tick")
      .throttle(1, 1.second)
      .statefulMapConcat { () =>
        var counter = 0L

        // we return the function that will be invoked for each element
        { _ =>
          counter += 1
          // we return an iterable with the single element
          (counter) :: Nil
        }
      }
      .map(id => {
        val quote = Quote.generateMarketData(id)
        log.info(s"$quote")
        quote
      })
      .map(quote => {
        val stringValue = Json.stringify(Json.toJson(quote))
        new ProducerRecord[String, String](
          quoteTopic,
          quote.company.symbol,
          stringValue
        )
      })
      .runWith(Producer.plainSink(producerSettings))
      .andThen {
        case e: Exception => log.error("Error", e)
        case _ =>
          println("Shutting down")
          EmbeddedKafka.stop()
      }

  }

}
