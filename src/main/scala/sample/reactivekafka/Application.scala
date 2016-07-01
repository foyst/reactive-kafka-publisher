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

  object ThrottledProducer {

    def produceThrottled(initialDelay: FiniteDuration, interval: FiniteDuration, messages: Seq[(String, String)]) = {

      val ticker = Source.tick(initialDelay, interval, Unit)
      val messagesSource = Source(messages.toList)

      //define a stream to bring it all together..
      val throttledStream = Source.fromGraph(GraphDSL.create() { implicit builder =>

        //define a zip operation that expects a tuple with a Tick and a Message in it..
        //(Note that the operations must be added to the builder before they can be used)
        val zip = builder.add(Zip[Unit.type, ProducerRecord[String, String]])

        //create a flow to extract the second element in the tuple (our message - we dont need the tick part after this stage)
        val messageExtractorFlow = builder.add(Flow[(Unit.type, ProducerRecord[String, String])].map(_._2))

        val producerRecordBuilder = builder.add(Flow[(String, String)].map(msg => new ProducerRecord[String, String](test_topic, msg._1, msg._2)))

        //import this so we can use the ~> syntax
        import GraphDSL.Implicits._

        //define the inputs for the zip function - it wont fire until something arrives at both inputs, so we are essentially
        //throttling the output of this steam
        ticker ~> zip.in0
        messagesSource ~> producerRecordBuilder ~> zip.in1

        //send the output of our zip operation to a processing messageExtractorFlow that just allows us to take the second element of each Tuple, in our case
        //this is the string message, we dont care about the Tick, it was just for timing and we can throw it away.
        //route that to the 'out' Sink.
        zip.out ~> messageExtractorFlow

        SourceShape(messageExtractorFlow.out)
      })
      throttledStream
    }
  }
}
