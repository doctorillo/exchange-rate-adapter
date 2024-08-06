package app

import com.typesafe.config.Config
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.pekko.kafka.ProducerSettings

object AdapterSink {

  def producerSettings(config: Config): ProducerSettings[String, String] = {
    val producerConfig = config.getConfig("pekko.kafka.producer")
    val bootstrap = config.getString("exchange-rate.producer.bootstrap-servers")
    ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)
      .withBootstrapServers(bootstrap)
  }

  def outboundTopic(implicit config: Config): String = config.getString("exchange-rate.producer.topic")
}
