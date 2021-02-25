package Utility

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import java.util.Properties
import Sources.CassandraSources._
import LoadTableUtility._
import org.apache.spark.sql.SparkSession

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

  val props: Properties = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("acks", "all")
  props.put("retries", "0")
  props.put("linger.ms", "1")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val pathForTransactions = "C:\\Users\\Alex\\Desktop\\Data_Bank\\historical_data.csv"
  //loadBankTransactions(DbHistoricalData)
  //readData(pathForTransactions)


  val pathForTransformed = "C:\\Users\\Alex\\Desktop\\Data_Bank\\incoming_data_transformed.csv"
  /*loadSimulationTransformed(DbTransformed)
  readData(pathForTransformed)*/


  val pathForPrediction = "C:\\Users\\Alex\\Desktop\\Data_Bank\\prediction.csv"
  loadSimulationPrediction(DbPrediction)
  readData(pathForPrediction)

  spark.streams.awaitAnyTermination()
}


