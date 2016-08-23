package sample.reactivekafka

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}

import akka.actor.ActorLogging
import akka.event.Logging
import akka.event.slf4j.Logger
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage._
import spray.json.JsValue
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration._

object TimeStretchGraphStage {

  def apply(timestretchFactor: Double, timestampFieldName: String) = new TimeStretchGraphStage(timestretchFactor, timestampFieldName)
}

class TimeStretchGraphStage(timestretchFactor: Double, timestampFieldName: String) extends GraphStage[FlowShape[JsValue, JsValue]] {

  val log = Logger("TimestampStretchTimerGraphStage")
  val in = Inlet[JsValue]("TimedGate.in")
  val out = Outlet[JsValue]("TimedGate.out")

  override def shape: FlowShape[JsValue, JsValue] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    new TimerGraphStageLogic(shape) {

      var upstreamCompleted = false
      var firstMessage = true

      var previousTimestamp: Option[Instant] = None
      var nextScheduledMessage: Option[JsValue] = None

      setHandler(in, new InHandler {

        override def onPush(): Unit = {
          val jsonMessage = grab(in)

          val msgTimestamp = Instant.parse(jsonMessage.asJsObject.fields(timestampFieldName).convertTo[String])

          if (firstMessage) {
            log.debug(s"On Push First Message: $jsonMessage")
            firstMessage = false
            previousTimestamp = Some(msgTimestamp)
            push(out, jsonMessage)
          } else {
            log.debug(s"On Push Subsequent Message: $jsonMessage")

            nextScheduledMessage = Some(jsonMessage)

            val timestampDifference = ChronoUnit.MILLIS.between(previousTimestamp.get, msgTimestamp)
            val msgDelay = (timestampDifference / timestretchFactor).milliseconds
            scheduleOnce(None, msgDelay)

            previousTimestamp = Some(msgTimestamp)
          }
        }

        override def onUpstreamFinish() = {
          log.debug("On Upstream Finish Called")
          upstreamCompleted = true
          completeIfReady()
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          log.debug("On Pull Called")
          completeIfReady()
          if (!readyToComplete) pull(in)
        }
      })

      override protected def onTimer(timerKey: Any): Unit = {

        log.debug(s"On Timer Called, pushing message: ${nextScheduledMessage.get}")
        nextScheduledMessage.foreach(push(out, _))
        nextScheduledMessage = None
      }

      private def completeIfReady(): Unit = {
        if (readyToComplete) {

          log.debug("Completing Stage")
          completeStage()
        }
      }

      def readyToComplete: Boolean = {
        !nextScheduledMessage.isDefined && !firstMessage && upstreamCompleted
      }
    }
  }

}
