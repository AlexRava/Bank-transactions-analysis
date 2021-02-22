import Sources.KafkaSources.{AllTransactionSource, InputSource, PredictionSource, TransactionTransformedSource}
import Streams.{StreamUtility, StreamingFlow}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object StartMonitoring extends App{

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

  /*var inputTopic = */

  println(InputSource.readFromSource())
  //MonitorTopic.fromTopicSource(InputSource).printData()



  var allTransactionsTopic = MonitorTopic.fromTopicSource(AllTransactionSource)
  var transactionTransformedTopic = MonitorTopic.fromTopicSource(TransactionTransformedSource)
  var predictionTopic = MonitorTopic.fromTopicSource(PredictionSource)
}
