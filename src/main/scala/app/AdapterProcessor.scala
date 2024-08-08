package app

import app.AdapterProcessor.SourceProtocol.TradeValue
import app.HttpSource.Kind
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

import java.math.{MathContext, RoundingMode}
import java.time.temporal.ChronoUnit
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

      def pairKey: String = s"$from$to"

      def isUSD: Boolean = from == "USD"

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

    final case class RateUpdated(issueDate: LocalDate,
                                 rates: Map[String, BigDecimal]) extends ProcessorMessage

    final case class SelectSource(kind: HttpSource.Kind) extends ProcessorMessage
  }

  private final case class ProcessorState(rates: SortedMap[LocalDate, Map[String, BigDecimal]],
                                          source: Source) {

    def get(date: LocalDate, pairKey: String): Option[BigDecimal] = {
      if (rateExists(date, pairKey)) {
        Some(rates(date)(pairKey))
      } else {
        None
      }
    }

    def rateExists(date: LocalDate, pairKey: String): Boolean = rates.keys.exists(_.isEqual(date)) && rates(date).keys.exists(_ == pairKey) && source.updatedAt.exists(_.plus(2, ChronoUnit.HOURS).isAfter(Instant.now())
    )

    def rateNotExists(date: LocalDate, pairKey: String): Boolean = !rateExists(date, pairKey)

    def updateReceived(kind: Kind, updatedAt: Option[Instant]): ProcessorState = copy(source = source.copy(kind = kind, updatedAt = updatedAt))

    def +(kv: (LocalDate, Map[String, BigDecimal])): ProcessorState = copy(rates = rates + kv, source = source.copy(updatedAt = Some(Instant.now())))
  }

  private object ProcessorState {
    def empty(): ProcessorState = ProcessorState(SortedMap.empty, Source(HttpSource.Primary))
  }

  private final case class Source(kind: HttpSource.Kind, updatedAt: Option[Instant] = None)

  private def errorPublisher(msg: ProcessorProtocol.ConvertAmount, replayTo: ActorRef[ProcessorProtocol.ProcessorMessage], error: String): Unit = {
    replayTo ! ProcessorProtocol.ConverterError(msg.issueDate, msg.amount, msg.from, msg.to, new RuntimeException(error))
  }

  def apply(): Behavior[ProcessorProtocol.ProcessorMessage] = Behaviors.setup(context => {
    import HttpSource._
    import ProcessorProtocol._

    implicit val actorClassicSystem: ActorSystem = context.system.classicSystem
    val mathCtx = new MathContext(5, RoundingMode.HALF_UP)

    val converter = (state: ProcessorState) => (msg: ConvertAmount, replyTo: ActorRef[ProcessorMessage]) => {
      state.get(msg.issueDate, msg.pairKey) match {
        case Some(rate) =>
          if (msg.isUSD) {
            replyTo ! AmountConverted(msg.issueDate, msg.amount.bigDecimal.multiply(rate.bigDecimal, mathCtx), msg.from, msg.to)
          } else {
            replyTo ! AmountConverted(msg.issueDate, msg.amount.bigDecimal.divide(rate.bigDecimal, mathCtx), msg.from, msg.to)
          }
        case None =>
          errorPublisher(msg, replyTo, "Converter. Exchange rate not found")
      }
    }

    def active(state: ProcessorState): Behavior[ProcessorMessage] = Behaviors.receiveMessage[ProcessorMessage] {
      case SelectSource(kind) =>
        context.log.info("select source received: {}", state.source.kind)
        active(state.updateReceived(kind, Some(Instant.now())))

      case RateUpdated(issueDate, rates) =>
        context.log.info("rate updated received: {}", state.source.kind)
        active(state + (issueDate -> rates))

      case rq@ConvertAmount(date, _, _, _, replyTo) if state.rateExists(date, rq.pairKey) =>
        context.log.info("convert amount received: {}", state.source.kind)
        converter(state)(rq, replyTo)
        Behaviors.same

      case rq@ConvertAmount(date, _, _, _, replyTo) if state.rateNotExists(date, rq.pairKey) && rq.isLive =>
        context.log.info("live exchange rate requested: {}", state.source.kind)
        val src = runLiveRatesRequest(state.source.kind)
        context.pipeToSelf(src) {
          case Success(value) => RateUpdated(date, value.quotes)
          case Failure(_) => SelectSource(HttpSource.Secondary)
        }
        errorPublisher(rq, replyTo, "Is Live. Exchange rate not found")
        Behaviors.same

      case rq@ConvertAmount(date, _, _, _, replyTo) if state.rateNotExists(date, rq.pairKey) && rq.isHistory =>
        context.log.info("historical exchange rate requested: {}", state.source.kind)
        val src = runHistoricalRates(date, state.source.kind);
        context.pipeToSelf(src) {
          case Success(value) => RateUpdated(date, value.quotes)
          case Failure(e) => {
            context.log.error("failed to get historical rates:", e)
            SelectSource(HttpSource.Secondary)
          }
        }
        errorPublisher(rq, replyTo, "Is History. Exchange rate not found")
        Behaviors.same

      case rq =>
        context.log.error("unexpected message: {}", rq)
        Behaviors.same
    }

    active(ProcessorState.empty())
  })

}
