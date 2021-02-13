package App

import Data.DataObject.{Transaction, TransactionFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object App extends App {



  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf: SparkConf = new SparkConf()
    .setAppName("structured-streaming-kafka-hello-world")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.sql.streaming.checkpointLocation", "checkpoint")

  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "input-topic")
    .load()

  val transactions = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String,String)]
    .map(_._2.split(",").toList)
     //.map(_.asInstanceOf[TransactionSerialized]) //non funziona perche lo split da in output una lista e non una tupla
    .map(Transaction(_))

    //.select("uid")

  //per ogni transactions si devono mettere in un topic kafka le relative transazioni





  val  printUid = transactions
    .writeStream
    .outputMode("update")
    .format("console")
    .start()

  printUid.awaitTermination()


}

