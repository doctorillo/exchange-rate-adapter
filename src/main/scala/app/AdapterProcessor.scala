package app

import app.AdapterProcessor.SourceProtocol.TradeValue
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

import java.math.{MathContext, RoundingMode}
import java.time.{Instant, LocalDate, ZoneId}
import scala.collection.immutable.SortedMap
import scala.util.{Failure, Success, Try}

object AdapterProcessor {
  object SourceProtocol {
    final case class TradeValue(marketId: Long,
                                selectionId: Long,
                                odds: Double,
                                stake: BigDecimal,
                                currency: String,
                                date: Instant)

    object TradeValue {

      import com.github.plokhotnyuk.jsoniter_scala.core._

      implicit val tradeValueCodec: JsonValueCodec[TradeValue] = JsonCodecMaker.make

      def fromJson(payload: String): Either[Throwable, TradeValue] = Try(readFromString[TradeValue](payload)).toEither

      implicit class TradeValueOps(val value: TradeValue) extends AnyVal {
        def toJson: Either[Throwable, String] = Try(writeToString(value)).toEither
      }
    }
  }

  object ProcessorProtocol {
    sealed trait ProcessorMessage

    final case class ConvertAmount(issueDate: LocalDate,
                                   amount: BigDecimal,
                                   from: String,
                                   to: String,
                                   replyTo: ActorRef[ProcessorMessage]) extends ProcessorMessage {
      def isLive: Boolean = issueDate.isEqual(LocalDate.now())

      def isHistory: Boolean = !isLive

    }

    object ConvertAmount {
      def make(payload: TradeValue, ref: ActorRef[ProcessorMessage]): ConvertAmount = ConvertAmount(LocalDate.ofInstant(payload.date, ZoneId.of("UTC")), payload.stake, payload.currency, "EUR", ref)
    }

    final case class AmountConverted(issueDate: LocalDate,
                                     amount: BigDecimal,
                                     from: String,
                                     to: String) extends ProcessorMessage

    final case class ConverterError(issueDate: LocalDate,
                                    amount: BigDecimal,
                                    from: String,
                                    to: String,
                                    error: Throwable) extends ProcessorMessage
  }

  private final case class SourceState(kind: HttpSource.Kind,
                                       requested: Option[Instant] = None)

  def apply(): Behavior[ProcessorProtocol.ProcessorMessage] = Behaviors.setup(context => {
    import ProcessorProtocol._

    implicit val actorClassicSystem: ActorSystem = context.system.classicSystem
    val mathCtx = new MathContext(5, RoundingMode.HALF_UP)

    val converter = (state: SortedMap[LocalDate, Map[String, BigDecimal]]) => (msg: ConvertAmount, replyTo: ActorRef[ProcessorMessage]) => {
      val pairKey = s"${msg.from}${msg.to}"
      if (state.keys.exists(_.isEqual(msg.issueDate)) && state(msg.issueDate).keys.exists(_ == pairKey)) {
        val rate = state(msg.issueDate)(pairKey)
        if (msg.from == "USD") {
          replyTo ! AmountConverted(msg.issueDate, msg.amount.bigDecimal.multiply(rate.bigDecimal, mathCtx), msg.from, msg.to)
        } else {
          replyTo ! AmountConverted(msg.issueDate, msg.amount.bigDecimal.divide(rate.bigDecimal, mathCtx), msg.from, msg.to)
        }
      } else {
        replyTo ! ConverterError(msg.issueDate, msg.amount, msg.from, msg.to, new RuntimeException("exchange rate not found"))
      }
    }

    def active(state: SortedMap[LocalDate, Map[String, BigDecimal]],
               source: SourceState
              ): Behavior[ProcessorMessage] = Behaviors.receiveMessage[ProcessorMessage] {
      case rq@ConvertAmount(date, amount, from, to, replyTo) if state.keys.exists(_.isEqual(date)) && state(date).keys.exists(_ == s"from$to") =>
        converter(state)(rq, replyTo)
        /*val rate = state(date)(s"from$to")
        if (from == "USD") {
          replyTo ! AmountConverted(date, amount.bigDecimal.multiply(rate.bigDecimal, mathCtx), from, to)
        } else {
          replyTo ! AmountConverted(date, amount.bigDecimal.divide(rate.bigDecimal, mathCtx), from, to)
        }*/
        Behaviors.same

      case rq@ConvertAmount(date, _, _, _, replyTo) if !state.keys.exists(_.isEqual(date)) && rq.isLive =>
        context.log.info("live exchange rate requested: {}", source.kind)
        import HttpSource._
        runLiveRatesRequestPrimary().onComplete {
          case Success(value) =>
            active(state + (date -> value.quotes), source.copy(requested = None))
            converter(state)(rq, replyTo)
          case Failure(_) =>
            runLiveRatesRequestSecondary().onComplete {
              case Success(value) =>
                active(state + (date -> value.quotes), source.copy(requested = None))
                converter(state)(rq, replyTo)
              case Failure(e) =>
                //converter(state)(rq, replyTo)
            }
        }
        Behaviors.same

      case rq@ConvertAmount(date, _, _, _, replyTo) if !state.keys.exists(_.isEqual(date)) && rq.isHistory =>
        context.log.info("historical exchange rate requested: {}", source.kind)

        import HttpSource._
        runHistoricalRatesPrimary(date).onComplete {
          case Success(value) =>
            val s = state + (date -> value.quotes)
            converter(s)(rq, replyTo)
            active(s, source.copy(requested = None))

          case Failure(exception) =>
            runHistoricalRatesSecondary(date).onComplete {
              case Success(value) =>
                val s = state + (date -> value.quotes)
                converter(s)(rq, replyTo)
                active(s, source.copy(requested = None))
              case Failure(e) =>
                converter(state)(rq, replyTo)
            }
        }
        Behaviors.same

      case value =>
        context.log.error("unexpected message: {}", value)
        Behaviors.same
    }

    active(SortedMap.empty, SourceState(HttpSource.Primary))
  })

}
