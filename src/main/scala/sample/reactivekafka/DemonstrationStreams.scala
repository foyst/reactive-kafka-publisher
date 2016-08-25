package sample.reactivekafka

import java.io.File

import akka.stream.scaladsl.{FileIO, Flow, Framing}
import akka.util.ByteString
import org.apache.kafka.clients.producer.ProducerRecord
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, JsString, JsonParser}

object DemonstrationStreams {

  val producerRecordBuilder = Flow[(String, (String, String))].map(msg => new ProducerRecord[String, String](msg._1, msg._2._1, msg._2._2))

  val lines = Flow[ByteString].via(Framing.delimiter(ByteString(System.lineSeparator), 10000, allowTruncation = true)).map(bs => bs.utf8String)

  private val timestampFieldName: String = "createdDate"
  private val metadataFieldName: String = "metadata"
  val tredIncidentMessagePipeline = FileIO.fromFile(new File("/Users/benfoster/Temp/20160823_TredIncidents.log"))
    .via(lines)
    .map(line => JsonParser(line).asJsObject)
    .map{jsValue =>

      val timestampValue = jsValue.asJsObject().fields(metadataFieldName).asJsObject.fields(timestampFieldName).convertTo[String]
      val correctedTimestamp = s"${timestampValue}Z"
      val metadataJsObjectFields = jsValue.asJsObject().fields(metadataFieldName).asJsObject.fields ++ Map(timestampFieldName -> JsString(correctedTimestamp))

      jsValue.asJsObject().copy(fields = jsValue.asJsObject().fields ++ Map(metadataFieldName -> JsObject(metadataJsObjectFields)))
    }
    .via(TimeStretchGraphStage(1, metadataFieldName + "." + timestampFieldName))
    .map(jsObject => (jsObject.asJsObject.fields("eventId").convertTo[String], jsObject.compactPrint))
    .map(keyAndMsg => ("tred_incidents", keyAndMsg))
    .via(producerRecordBuilder)
}
