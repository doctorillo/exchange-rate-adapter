package app

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.pekko.Done
import org.apache.pekko.kafka.ProducerMessage.Envelope
import org.apache.pekko.kafka.scaladsl.Producer
import org.apache.pekko.kafka.{CommitterSettings, ConsumerMessage, ProducerMessage, ProducerSettings}
import org.apache.pekko.stream.scaladsl.Sink

import scala.concurrent.Future

object AdapterSink {

  def producerSettings(config: Config): ProducerSettings[String, String] = {
    val producerConfig = config.getConfig("pekko.kafka.producer")
    val bootstrap = config.getString("exchange-rate.producer.bootstrap-servers")
    ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)
      .withBootstrapServers(bootstrap)
  }

  def outboundTopic(implicit config: Config): String = config.getString("exchange-rate.producer.topic")

  val toProducerMessage: ((String, String, String, Long)) => Envelope[String, String, Long] = {
    case (topic, key, value, offset) => ProducerMessage.single(new ProducerRecord(topic, key, value), offset)
  }

  val toProducerRecord: ((String, String, String, Long)) => Envelope[String, String, Long] = {
    case (topic, key, value, offset) => ProducerMessage.single(new ProducerRecord(topic, key, value), offset)
  }
}
