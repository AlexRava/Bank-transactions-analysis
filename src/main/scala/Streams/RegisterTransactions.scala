package Streams

import SinkConnector.{CassandraSink, SinkTransaction}
import Sources.CassandraSources.DbHistoricalData
import Sources.{CassandraSource, KafkaSource}
import Sources.KafkaSources.InputSource
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, streaming}

object RegisterTransactions extends AbstractStreamingFlow {

  var inputSource: KafkaSource = InputSource
  var outputSource: CassandraSource = DbHistoricalData

  override protected def compute(): DataFrame = inputSource.readFromSource()

  override protected def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] = compute()
    .writeStream
    .outputMode("update")
    .foreach(new SinkTransaction(outputSource).asInstanceOf[ForeachWriter[Row]])
}
