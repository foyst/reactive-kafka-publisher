package sample.reactivekafka

import java.nio.file.Paths

import akka.actor.{ActorSystem, Props}
import akka.stream.{ActorMaterializer, ClosedShape, DelayOverflowStrategy, SourceShape}
import akka.stream.scaladsl._
import akka.kafka
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.util.ByteString
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

  val producerRecordBuilder = Flow[(String, String)].map(msg => new ProducerRecord[String, String](test_topic, msg._1, msg._2))

  val testStream = ThrottledProducer.produceThrottled(5 seconds, 5 seconds, testKeyList(null))
    .via(producerRecordBuilder)
    .runWith(Producer.plainSink(producerSettings))

//  Example below for how to stream from file and delimit by new line
//  val file = Paths.get("file path here")
//
//  val testStream = FileIO.fromPath(file)
//    .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 100000))
//    .map(_.utf8String)
//    .map(msg => new ProducerRecord[String, String](test_topic, null, msg))
//    .runWith(Producer.plainSink(producerSettings))
}
