package Utility

import java.io.File
import java.util.Properties

import App.App.spark
import Data.DataObject.Transaction
import SinkConnector.CassandraSink
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


  val reader = CSVReader.open(new File("C:\\Users\\Alex\\Desktop\\Fraud_Historical_data\\historical_data.csv"))

  val producer: Producer[String, String] = new KafkaProducer(props)
  reader.foreach(transaction => {
    producer.send(new ProducerRecord[String,String]("load-table-topic", transaction.mkString(",")))
    Thread.sleep(N_MILLISEC_IN_SEC * 0)
    }
  )
  import spark.implicits._

  val loadData = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "load-table-topic")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String,String)]
    .map(_._2.split(",").toList)
    //.map(TransactionTransformed(_))
    .writeStream
    .outputMode(OutputMode.Append)
    .foreach(new CassandraSink())
    .start()

  loadData.awaitTermination()
}
