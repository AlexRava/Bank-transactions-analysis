package Streams

import Data.{DataFactory, Transaction}
import SinkConnector.{CassandraSink, SinkTransaction}
import Sources.CassandraSources.DbHistoricalData
import Sources.{CassandraSource, KafkaSource}
import Sources.KafkaSources.InputSource
import App.Application.spark
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, streaming}

object RegisterTransactions extends AbstractStreamingFlow {

  import spark.implicits._

  var inputSource: KafkaSource = InputSource
  var outputSource: CassandraSource = DbHistoricalData

  override protected def compute(): DataFrame = inputSource
    .readFromSource()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

  override protected def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] = compute()
    .as[(String,String)]
    .map(_._2.split(",").toList)
    .map(DataFactory.createTransaction(_))
    .writeStream
    .outputMode("update")
    .foreach((new SinkTransaction(outputSource)))
    .asInstanceOf[streaming.DataStreamWriter[Row]]
}