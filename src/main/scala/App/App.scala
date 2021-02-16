package App

import Data.DataObject.{Transaction, TransactionFactory}
import Streams.{AllUsersTransactions, InputStream, StreamUtility, StreamingFlow, allTransactions}
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.cassandra._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.apache.spark.sql.functions._

object App extends App {
  def printStream(stream: StreamingFlow) = StreamUtility.printInStdOut(stream.readStream)

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

  //import spark.implicits._

  val transaction = InputStream.readStream//transaction

  AllUsersTransactions.allUserTransactions(transaction).start()
  
  printStream(allTransactions)

  spark.streams.awaitAnyTermination()
}


