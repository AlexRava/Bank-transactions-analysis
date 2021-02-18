package App

import Data.DataObject.{Transaction, TransactionFactory}
import Streams.{AllTransactions, AllUsersTransactions, InputStream, StreamUtility, StreamingFlow, TransformedStream}
import Transformer.{DataTransformer, Transformer}
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.cassandra._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.apache.spark.sql.functions._

object App extends App {
  def printStream(stream: StreamingFlow) = StreamUtility.printInStdOut(stream.readStream)
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

  //import spark.implicits._

  val transaction = InputStream.readStream//transaction

  AllUsersTransactions.allUserTransactions(transaction).start()

  //transform data
  val transformer: Transformer = new DataTransformer()
  transformer.addSource(AllTransactions)
  transformer.addSource(InputStream)
  val t = transformer.compute()


  // And load it back in during production
  val pathOneHotModel = "/tmp/spark-logistic-regression-model"
  val oneHotModel = PipelineModel.load(pathOneHotModel)
val encoder = new OneHotEncoder()
  /*val encoder = new OneHotEncoder()
    .setInputCol("categoryIndex")
    .setOutputCol("categoryVec")*/

  val encoded = encoder.transform(indexed)
  encoded.select("id", "categoryVec").show()
  // $example off$
  sc.stop()



















  printStream(TransformedStream)

  //printStream(t)

  spark.streams.awaitAnyTermination()
}


