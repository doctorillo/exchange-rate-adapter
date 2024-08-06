package app

import app.AdapterProcessor.ProcessorProtocol
import app.AdapterProcessor.ProcessorProtocol.{AmountConverted, ConvertAmount, ConverterError, ProcessorMessage}
import app.AdapterProcessor.SourceProtocol.TradeValue
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.apache.pekko.actor.typed.{Scheduler, ActorSystem => TypedActorSystem}
import org.apache.pekko.kafka.ConsumerMessage._
import org.apache.pekko.kafka.ProducerMessage
import org.apache.pekko.kafka.scaladsl.Consumer.Control
import org.apache.pekko.kafka.scaladsl.{Consumer, Transactional}
import org.apache.pekko.stream.RestartSettings
import org.apache.pekko.stream.scaladsl.{Flow, RestartSource, Sink}
import org.apache.pekko.util.Timeout

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

object Main extends App {

  val transactionalId = "exchange-rate-processor-tx1"

  val typedSystem: TypedActorSystem[ProcessorMessage] = TypedActorSystem[ProcessorProtocol.ProcessorMessage](AdapterProcessor(), "exchange-rate-actor-system")
  implicit val config: Config = ConfigFactory.load()
  implicit val classicSystem: ActorSystem = typedSystem.classicSystem
  implicit val ec: ExecutionContext = typedSystem.executionContext
  implicit val timeout: Timeout = 3.seconds
  implicit val sc: Scheduler = typedSystem.scheduler

  val innerControl = new AtomicReference[Control](Consumer.NoopControl)

  val unmarshallFlow: Flow[TransactionalMessage[String, String], (TransactionalMessage[String, String], TradeValue), NotUsed] = Flow.fromFunction((msg: TransactionalMessage[String, String]) => TradeValue.fromJson(msg.record.value()) match {
    case Right(value) => (msg, value)
    case Left(error) => throw error
  })
  val logic = Flow[(TransactionalMessage[String, String], TradeValue)].mapAsync(1) {
    case (msg, out) => typedSystem.ask(ref => ConvertAmount.make(out, ref))
      .map {
        case AmountConverted(_, amount, _, to) => out.copy(stake = amount, currency = to).toJson match {
          case Right(value) => ProducerMessage.single(new ProducerRecord[String, String](AdapterSink.outboundTopic, msg.record.partition(), msg.record.offset(), msg.record.key, value), msg.partitionOffset)
          case Left(error) => throw error
        }
        case ConverterError(_, _, _, _, thr) => throw thr
        case response => throw new RuntimeException(s"Unexpected response: $response")
      }
  }

  val stream = RestartSource.onFailuresWithBackoff(
    RestartSettings(
      minBackoff = 1.seconds,
      maxBackoff = 15.seconds,
      randomFactor = 0.2)) { () =>
    AdapterSource.make
      .via(unmarshallFlow)
      .via(logic)
      .mapMaterializedValue(c => innerControl.set(c))
      .via(Transactional.flow(AdapterSink.producerSettings(config), transactionalId))
  }
  stream.runWith(Sink.ignore)

  sys.ShutdownHookThread {
    Await.result(innerControl.get.shutdown(), 15.seconds)
  }
}