package App

import Monitor.SystemMonitor
import Sources.KafkaSources.{AllTransactionSource, InputSource}
import Streams.{DataTransformer, InputStream, Predict, RegisterTransactions}
import Utility.{MergeStrategy, StreamUtility}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object Application extends App {

  def printStream(stream: DataFrame) = StreamUtility.printInStdOut(stream)

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

  DataTransformer.addSource(InputSource)
  DataTransformer.addSource(AllTransactionSource)
  DataTransformer.setMergeStrategy(_.get(InputSource.name).get) //transformer with a simple strategy
  //DataTransformer.setMergeStrategy(MergeStrategy.simpleStrategy(_))

  InputStream.startFlow()
  RegisterTransactions.startFlow()
  DataTransformer.startFlow()
  Predict.startFlow()

  //Thread.sleep(1000)
  SystemMonitor.printPrediction()
  //SystemMonitor.monitorTransactionTransformed()
  spark.streams.awaitAnyTermination()
}


