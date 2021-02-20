package Streams

import App.App.spark
import org.apache.spark.sql.{DataFrame, Row, streaming}

object RegisterTransactions extends StreamingFlow {

  import spark.implicits._

  override def readData(): DataFrame = InputStream.readData()

  override def compute(): DataFrame = readData()

  override def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] =
}
