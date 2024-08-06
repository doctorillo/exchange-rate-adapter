package app

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.pekko.kafka.scaladsl.Consumer.Control
import org.apache.pekko.kafka.scaladsl.Transactional
import org.apache.pekko.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import org.apache.pekko.stream.scaladsl.Source

object AdapterSource {
  def make(implicit config: Config): Source[ConsumerMessage.TransactionalMessage[String, String], Control] = {
    val inboundTopic: String = config.getString("exchange-rate.consumer.topic")
    val consumerConfig = config.getConfig("pekko.kafka.consumer")
    val appConfig = config.getConfig("exchange-rate.consumer")
    val bootstrap = appConfig.getString("bootstrap-servers")
    val consumerGroup = appConfig.getString("group-id")
    val consumerSettings = ConsumerSettings(consumerConfig, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrap)
      .withGroupId(consumerGroup)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    Transactional.source(consumerSettings, Subscriptions.topics(inboundTopic))
  }
}
