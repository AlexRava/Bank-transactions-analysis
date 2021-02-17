package Utility

import java.io.File
import java.util.Properties

import App.App.spark
import Data.DataObject.{Transaction, TransactionFactory, TransactionTransformed}
import SinkConnector.{CassandraSink, CassandraSinkTransformed}
import Streams.StreamUtility
import com.github.tototoshi.csv.CSVReader
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.cassandra._


object LoadTableInCassandra extends App{

  private val N_MILLISEC_IN_SEC = 1000

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf: SparkConf = new SparkConf()
    .setAppName("Bank-transactions-analysis")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.sql.streaming.checkpointLocation", "checkpoint")

  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  private val props: Properties = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("acks", "all")
  props.put("retries", "0")
  props.put("linger.ms", "1")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


  val path = "C:\\Users\\Alex\\Desktop\\Fraud_Historical_data\\test_transformed.csv"
  val reader = CSVReader.open(new File(path))

  import spark.implicits._

  val loadData = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "load-table-topic1")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String,String)]
    .map(_._2.split(",").toList)
    .map(TransactionFactory.createTransactionTransformed(_))
    .writeStream
    .outputMode(OutputMode.Append)
    .foreach(new CassandraSinkTransformed())
    .start()
  //StreamUtility.printInStdOut(loadData)

  Thread.sleep(2000)
  println(" start sending data")
  var cont = 0
  val producer: Producer[String, String] = new KafkaProducer(props)
  reader.foreach(transaction => {
    cont = cont + 1
    producer.send(new ProducerRecord[String,String]("load-table-topic1", transaction.mkString(",")))
    //Thread.sleep(N_MILLISEC_IN_SEC * 0)
  }
  )
  println("this is the end, row:",cont)
    /*
    .writeStream
    .outputMode(OutputMode.Append)
    .foreach(new CassandraSinkTransformed())
    .start()
*/
  spark.streams.awaitAnyTermination()
}
