package Utility

import java.io.File
import java.util.Properties

import SinkConnector.{CassandraSink, SinkTransformed}
import Sources.CassandraSources.DbHistoricalData
import Utility.LoadTableUtility._
import com.github.tototoshi.csv.CSVReader
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.cassandra._


object LoadTableInCassandra extends App{

  val TOPIC_NAME = "load-table-topic1"

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








  Thread.sleep(2000)


  val pathForTransactions = "C:\\Users\\Alex\\Desktop\\Fraud_Historical_data\\test_transformed.csv"
  val source1 =

  val pathForResults = ""
  val source2 =

  val pathForTransformed =""
  val source3 =

  val pathForPrediction = ""
  val source4 =


  readData(pathForTransactions, )

  spark.streams.awaitAnyTermination()
}


