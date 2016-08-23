package sample.reactivekafka

import akka.actor.{ ActorSystem, Props }
import akka.stream.{ ActorMaterializer, ClosedShape, DelayOverflowStrategy, SourceShape }
import akka.stream.scaladsl._
import akka.kafka
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.duration._

object Application extends App {

  implicit val system = ActorSystem("ReactiveKafkaPublisher")

  implicit val materializer = ActorMaterializer()(system)

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer).withBootstrapServers("192.168.99.102:9000")

  val test_topic: String = "test_topic"
  private val key1: String = "12345"
  private val key2: String = "98765"

  def testKeyList(key: String) = (1 to 5000).map(n => (key, s"test${n}"))

  def messageTickerStream(messages: Seq[(String, String)]) = {
    RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>

      val source = builder.add(ThrottledProducer.produceThrottled(5 seconds, 5 seconds, messages))
      val sink = Producer.plainSink(producerSettings)

      import GraphDSL.Implicits._

      source ~> sink

      ClosedShape
    })
  }

  messageTickerStream(testKeyList(null)).run()
}
