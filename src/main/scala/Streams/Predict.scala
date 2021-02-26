package Streams
import App.Application.spark
import Data.{DataFactory, Transaction, TransactionTransformed}
import Sources.CassandraSources.DbPrediction
import Sources.KafkaSource
import Sources.KafkaSources.{AllTransactionSource, InputSource, PredictionSource, TransactionTransformedSource}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Row, streaming}
import org.apache.spark.sql.functions.{from_json, struct, to_json}

object Predict extends AbstractStreamingFlow {

  val inputSource: KafkaSource = TransactionTransformedSource
  val outputSource: KafkaSource = PredictionSource

  import spark.implicits._
  import Utility.DataFrameExtension._

  override protected def compute(): DataFrame =  TransactionTransformedSource
    .readFromSource()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String,String)]
    .select($"key",from_json($"value",TransactionTransformed.TransactionTransformedSchema) as "data")
    .select($"data.uid",$"data.TransactionID")
    //.select("*")
    .addPrediction()


  override protected def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] = {
    val retrievePrediction = (batchDF: DataFrame, batchId: Long) =>
      batchDF.collect.foreach(user => new RetrievePrediction(s"${user(0)}", s"${user(1)}", DbPrediction, outputSource).startFlow())

    compute
      .writeStream
      .outputMode(OutputMode.Update)
      .foreachBatch( retrievePrediction )
  }




}
