package sample.reactivekafka

import akka.stream.SourceShape
import akka.stream.scaladsl.{Flow, GraphDSL, Source, Zip}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration.FiniteDuration

object ThrottledProducer {

  def produceThrottled(initialDelay: FiniteDuration, interval: FiniteDuration, messages: Seq[(String, String)]) = {

    val ticker = Source.tick(initialDelay, interval, Unit)
    val messagesSource = Source(messages.toList)

    //define a stream to bring it all together..
    val throttledStream = Source.fromGraph(GraphDSL.create() { implicit builder =>

      //define a zip operation that expects a tuple with a Tick and a Message in it..
      //(Note that the operations must be added to the builder before they can be used)
      val zip = builder.add(Zip[Unit.type, (String, String)])

      //create a flow to extract the second element in the tuple (our message - we dont need the tick part after this stage)
      val messageExtractorFlow = builder.add(Flow[(Unit.type, (String, String))].map(_._2))

      //import this so we can use the ~> syntax
      import GraphDSL.Implicits._

      //define the inputs for the zip function - it wont fire until something arrives at both inputs, so we are essentially
      //throttling the output of this steam
      ticker ~> zip.in0
      messagesSource ~> zip.in1

      //send the output of our zip operation to a processing messageExtractorFlow that just allows us to take the second element of each Tuple, in our case
      //this is the string message, we dont care about the Tick, it was just for timing and we can throw it away.
      //route that to the 'out' Sink.
      zip.out ~> messageExtractorFlow

      SourceShape(messageExtractorFlow.out)
    })
    throttledStream
  }
}