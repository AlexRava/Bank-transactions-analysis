package InputSimulation

import java.util.Properties

import Sources.KafkaSources.InputSource
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}

trait Simulation [T]{

  def init()

  def start( dataStream : DataSource[T])

}

object UsersSimulation extends Simulation[Seq[String]]{

  private val SEPARATOR = ","
  private val WAITING_TIME = 1000

  private val props: Properties = new Properties()

  override def init(): Unit = {
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", "0");
    props.put("linger.ms", "1");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  }

  override def start(source : DataSource[Seq[String]]): Unit = {

    val producer: Producer[String, String] = new KafkaProducer(props)

    source.getDataStream().foreach(transaction => {
      producer.send(new ProducerRecord[String,String](InputSource.topic, transaction.mkString(SEPARATOR)))
      Thread.sleep(WAITING_TIME)
    })

  }


}

