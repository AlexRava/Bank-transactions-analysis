package App

import Data.DataObject.{Transaction, TransactionFactory}
import Sources.KafkaSources.{AllTransactionSource, InputSource}
import Streams.{InputStream, Predict, RegisterTransactions, StreamUtility, StreamingFlow, StreamingFlowWithMultipleSources}
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

object Application extends App {

  def printStream(stream: StreamingFlow) = StreamUtility.printInStdOut(stream.compute())
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



  //var input = new InputStream(InputSource, AllTransactionSource) //fare una factory settando le source
  //input.startFlow()

  InputStream.startFlow()

  //RegisterTransactions.startFlow()

  val transformer: StreamingFlowWithMultipleSources = new DataTransformer()

  transformer.addSource("INPUT_DATA", InputStream.readData())
  printStream(transformer)
/*
  Predict.startFlow()

  */


















  //transformer.addSource()
  //val model: Model = new Model()
  /*val t = transformer.compute()
  val transformed_transaction = TransformedStream.write()
  transformed_transaction.writeStream.foreachBatch( (batchDF: DataFrame, batchId:Long) => {
    batchDF.foreach(row => println(row))
  }).start()

  //printStream(transformed_transaction)

  /*val res = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "results")
    .load()
  printStream(res)*/


  // And load it back in during production
  /*val pathOneHotModel = "/tmp/spark-logistic-regression-model"
  val oneHotModel = PipelineModel.load(pathOneHotModel)
  val encoder = new OneHotEncoder()*/
  /*val encoder = new OneHotEncoder()
    .setInputCol("categoryIndex")
    .setOutputCol("categoryVec")

  val encoded = encoder.transform(indexed)
  encoded.select("id", "categoryVec").show()
  // $example off$
  sc.stop()*/
*/

  //printStream(TransformedStream)
  //printStream(t)

  spark.streams.awaitAnyTermination()
}


