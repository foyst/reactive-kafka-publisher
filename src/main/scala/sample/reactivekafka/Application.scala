package sample.reactivekafka

import akka.actor.{ActorSystem, Props}
import akka.stream._
import akka.stream.scaladsl._
import akka.kafka
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import sample.reactivekafka.DemonstrationStreams._

import scala.concurrent.duration._

object Application extends App {

  implicit val system = ActorSystem("ReactiveKafkaPublisher")

  implicit val materializer = ActorMaterializer()(system)

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer).withBootstrapServers("192.168.99.103:9000")

  val materialisedStream = tredIncidentMessagePipeline
//      .viaMat(KillSwitches.single)(Keep.right)
    .to(Producer.plainSink(producerSettings)).run
}
