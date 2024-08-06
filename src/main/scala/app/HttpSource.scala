package app

import com.github.pjfanning.pekkohttpjsoniterscala.JsoniterScalaSupport
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.client.RequestBuilding.Get
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

object HttpSource {
  sealed trait Kind

  final case object Primary extends Kind

  final case object Secondary extends Kind

  case class LiveRates(success: Boolean,
                       terms: String,
                       privacy: String,
                       timestamp: Long,
                       source: String,
                       quotes: Map[String, BigDecimal])

  object LiveRates {
    implicit val liveRatesCodec: JsonValueCodec[LiveRates] = JsonCodecMaker.make
  }

  case class HistoricalRates(success: Boolean,
                             terms: String,
                             privacy: String,
                             historical: Boolean,
                             date: LocalDate,
                             timestamp: Long,
                             source: String,
                             quotes: Map[String, BigDecimal])

  object HistoricalRates {
    implicit val historicalRatesCodec: JsonValueCodec[HistoricalRates] = JsonCodecMaker.make
  }

  private val liveUrl = ConfigFactory.load().getString("exchange-rate.http-source-primary.live-url")
  private val historyUrl = ConfigFactory.load().getString("exchange-rate.http-source-primary.history-url")
  private val makeLiveRatesRequest = Get(liveUrl)
  private val makeHistoryRatesRequest: String => HttpRequest = date => Get(s"$historyUrl&date=$date")
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  def runLiveRatesRequestPrimary()(implicit system: ActorSystem): Future[LiveRates] = {
    import JsoniterScalaSupport._
    import LiveRates._

    Http().singleRequest(makeLiveRatesRequest).flatMap {
      case HttpResponse(StatusCodes.OK, _, entity, _) =>
        Unmarshal(entity).to[LiveRates]
      case HttpResponse(status, _, _, _) =>
        throw new RuntimeException(s"Failed to get live rates. Status code: $status")
    }
  }

  def runLiveRatesRequestSecondary()(implicit system: ActorSystem): Future[LiveRates] = runLiveRatesRequestPrimary()

  def runHistoricalRatesPrimary(date: LocalDate)(implicit system: ActorSystem): Future[HistoricalRates] = {
    import HistoricalRates._
    import JsoniterScalaSupport._

    Http().singleRequest(makeHistoryRatesRequest(date.format(DateTimeFormatter.ISO_LOCAL_DATE))).flatMap {
      case HttpResponse(StatusCodes.OK, _, entity, _) =>
        Unmarshal(entity).to[HistoricalRates]
      case HttpResponse(status, _, _, _) =>
        throw new RuntimeException(s"Failed to get live rates. Status code: $status")
    }
  }

  def runHistoricalRatesSecondary(date: LocalDate)(implicit system: ActorSystem): Future[HistoricalRates] = runHistoricalRatesPrimary(date)
}
