import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.Properties
import scala.io.StdIn.{readInt, readLine}
object KafkaProducer2 extends App {


    val props:Properties=new Properties();
    props.setProperty("bootstrap.servers","localhost:9092")
    props.setProperty("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer")
    props.setProperty("value.serializer","org.apache.kafka.common.serialization.IntegerSerializer")
    val producer:KafkaProducer[Int,Int]=new KafkaProducer[Int,Int](props)
    for(i <- 1 to 100000){
      println("please enter key")
      val key=readInt()
      println("please enter value")
      val value=readInt()

      producer.send(new ProducerRecord[Int,Int]("inputTopic",key,
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
