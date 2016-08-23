package sample.reactivekafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.WordSpecLike
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.duration._

class TimeStretchGraphStageSpec extends TestKit(ActorSystem("timeStretchGraphStageSpec")) with WordSpecLike {

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  "TimeStretchGraphStage" should {

    "Emit messages in realtime when time stretch factor is set to 1" in {

      val jsonMessage1 = """{"timestamp": "2016-08-19T04:40:00.000Z"}""".parseJson
      val jsonMessage2 = """{"timestamp": "2016-08-19T04:40:02.000Z"}""".parseJson
      val jsonMessages = Vector(jsonMessage1, jsonMessage2)

      Source(jsonMessages).via(TimeStretchGraphStage(1, "timestamp"))
        .runWith(TestSink.probe[JsValue])
        .request(2)
        .expectNext(100 milliseconds, jsonMessage1) //First message should come through immediately
        .expectNext(2100 milliseconds, jsonMessage2)
    }

    "Emit messages twice as fast as realtime when time stretch factor is set to 2.0" in {

      val jsonMessage1 = """{"timestamp": "2016-08-19T04:40:00.000Z"}""".parseJson
      val jsonMessage2 = """{"timestamp": "2016-08-19T04:40:02.000Z"}""".parseJson
      val jsonMessages = Vector(jsonMessage1, jsonMessage2)

      Source(jsonMessages).via(TimeStretchGraphStage(2.0, "timestamp"))
        .runWith(TestSink.probe[JsValue])
        .request(2)
        .expectNext(100 milliseconds, jsonMessage1) //First message should come through immediately
        .expectNext(1100 milliseconds, jsonMessage2)
    }

    "Emit messages four times faster than realtime when time stretch factor is set to 4.0" in {

      val jsonMessage1 = """{"timestamp": "2016-08-19T04:40:00.000Z"}""".parseJson
      val jsonMessage2 = """{"timestamp": "2016-08-19T04:40:02.000Z"}""".parseJson
      val jsonMessages = Vector(jsonMessage1, jsonMessage2)

      Source(jsonMessages).via(TimeStretchGraphStage(4.0, "timestamp"))
        .runWith(TestSink.probe[JsValue])
        .request(2)
        .expectNext(100 milliseconds, jsonMessage1) //First message should come through immediately
        .expectNext(600 milliseconds, jsonMessage2)
    }

    "Emit messages twice as slow as realtime when time stretch factor is set to 0.5" in {

      val jsonMessage1 = """{"timestamp": "2016-08-19T04:40:00.000Z"}""".parseJson
      val jsonMessage2 = """{"timestamp": "2016-08-19T04:40:02.000Z"}""".parseJson
      val jsonMessages = Vector(jsonMessage1, jsonMessage2)

      Source(jsonMessages).via(TimeStretchGraphStage(0.5, "timestamp"))
        .runWith(TestSink.probe[JsValue])
        .request(2)
        .expectNext(100 milliseconds, jsonMessage1) //First message should come through immediately
        .expectNoMsg(4 seconds)
        .expectNext(jsonMessage2)
    }

    "Emit messages four times slower than realtime when time stretch factor is set to 0.25" in {

      val jsonMessage1 = """{"timestamp": "2016-08-19T04:40:00.000Z"}""".parseJson
      val jsonMessage2 = """{"timestamp": "2016-08-19T04:40:02.000Z"}""".parseJson
      val jsonMessages = Vector(jsonMessage1, jsonMessage2)

      Source(jsonMessages).via(TimeStretchGraphStage(0.25, "timestamp"))
        .runWith(TestSink.probe[JsValue])
        .request(2)
        .expectNext(100 milliseconds, jsonMessage1) //First message should come through immediately
        .expectNoMsg(8 seconds)
        .expectNext(1 second, jsonMessage2)
    }

    "Emit messages five times slower than realtime when time stretch factor is set to 0.2" in {

      val jsonMessage1 = """{"timestamp": "2016-08-19T04:40:00.000Z"}""".parseJson
      val jsonMessage2 = """{"timestamp": "2016-08-19T04:40:02.000Z"}""".parseJson
      val jsonMessages = Vector(jsonMessage1, jsonMessage2)

      Source(jsonMessages).via(TimeStretchGraphStage(0.2, "timestamp"))
        .runWith(TestSink.probe[JsValue])
        .request(2)
        .expectNext(100 milliseconds, jsonMessage1) //First message should come through immediately
        .expectNoMsg(10 seconds)
        .expectNext(1 second, jsonMessage2)
    }

    "Complete stream once run out of messages" in {

      val jsonMessage1 = """{"timestamp": "2016-08-19T04:40:00.000Z"}""".parseJson
      val jsonMessage2 = """{"timestamp": "2016-08-19T04:40:02.000Z"}""".parseJson
      val jsonMessages = Vector(jsonMessage1, jsonMessage2)

      Source(jsonMessages).via(TimeStretchGraphStage(2.0, "timestamp"))
        .runWith(TestSink.probe[JsValue])
        .request(3)
        .expectNext(100 milliseconds, jsonMessage1) //First message should come through immediately
        .expectNext(1100 milliseconds, jsonMessage2)
        .expectComplete()
    }
  }
}
