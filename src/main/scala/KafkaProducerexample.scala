import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.Properties
import scala.io.StdIn.{readInt, readLine}

object KafkaProducerexample extends App {
  val props:Properties=new Properties();
  props.setProperty("bootstrap.servers","localhost:9092")
  props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  props.setProperty("value.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
  val producer:KafkaProducer[String,Integer]=new KafkaProducer[String,Integer](props)
  for(i <- 1 to 100000){
    println("please enter key")
    val key=readLine()
    println("please enter value")
    val value=readInt()

  producer.send(new ProducerRecord[String,Integer]("inputTopic",key,
    value),
    (m: RecordMetadata, e: Exception) => {
    if (e != null) {
      println("Exception")
    } else {
      println("Sucessfully sent")
    }
  });
  }
}
