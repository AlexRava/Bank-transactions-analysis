package Streams

import App.Application.spark
import org.apache.spark.sql.{DataFrame, Row, streaming}
import Data.DataFactory
import Sources.CassandraSources.DbHistoricalData
import Sources.KafkaSources.{AllTransactionSource, InputSource}
import Sources.KafkaSource
import org.apache.spark.sql.streaming.OutputMode

object InputStream extends AbstractStreamingFlow {

  val inputSource: KafkaSource = InputSource
  val outputSource: KafkaSource = AllTransactionSource

  import spark.implicits._

  override protected def compute() =
    inputSource.readFromSource()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String,String)]
    .map(_._2.split(",").toList)
    .map(DataFactory.createTransaction(_))
    .select($"uid")

  override protected def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] = {
      val retrieveDataforEachUsersInBatch =
      (batchDF: DataFrame, batchId:Long) => batchDF.collect.foreach(
          userInARow => new RetrieveAllTransactionsOf(userInARow.mkString, DbHistoricalData , outputSource).startFlow()
      )

    compute()
      .writeStream
      .outputMode(OutputMode.Update)
      .foreachBatch(retrieveDataforEachUsersInBatch)
  }
}
