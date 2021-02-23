package Streams

import App.Application.spark
import Data.DataObject.Transaction
import SinkConnector.CassandraSink
import Sources.KafkaSource
import Sources.KafkaSources.InputSource
import org.apache.spark.sql.{DataFrame, Row, streaming}

object RegisterTransactions extends AbstractStreamingFlow {

  var inputSource: KafkaSource = InputSource
  import spark.implicits._

  //override def readData(): DataFrame = InputStream.readData()

  override protected def compute(): DataFrame = inputSource.readFromSource()

  override protected def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] = compute()
    .writeStream
    .outputMode("update")
    .foreach(new CassandraSink())
}
