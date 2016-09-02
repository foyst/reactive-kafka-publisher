package sample.reactivekafka

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.stream._
import akka.stream.scaladsl._
import akka.kafka
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import sample.reactivekafka.DemonstrationStreams._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration._
import KafkaConfig.kafkaProducerSink
import akka.util.ByteString

import scala.concurrent.Await

object KafkaConfig {

  def kafkaProducerSink(implicit actorSystem: ActorSystem) = Producer.plainSink(ProducerSettings(actorSystem, new StringSerializer, new StringSerializer).withBootstrapServers("192.168.99.103:9000"))
}

object Application extends App {

  implicit val system = ActorSystem("ReactiveKafkaPublisher")

  implicit val materializer = ActorMaterializer()(system)

  val materialisedStream = tredIncidentMessagePipeline
    .to(kafkaProducerSink).run
}

object MonitoringApplication extends App {

  implicit val system = ActorSystem("MonitoringMetricsPublisher")

  implicit val materializer = ActorMaterializer()(system)

  val materialisedStream = monitoringMetricsPipeline
    .to(kafkaProducerSink).run
}
