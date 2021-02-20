package Streams

import App.App.spark
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Row, streaming}

trait Flow {

  def readData(): DataFrame
  def compute(): DataFrame
  def writeData[T <: AnyRef](): AnyRef

  //def writeData [T]():T
  //def writeData[Any]():Any

  def initFlow()

}

trait FinishedFlow extends Flow{

  override def writeData[DataFrameWriter[Row]](): org.apache.spark.sql.DataFrameWriter[Row]

  override def initFlow() = writeData.save()


}

trait StreamingFlow extends Flow{

  override def writeData[DataStreamWriter[Row]](): org.apache.spark.sql.streaming.DataStreamWriter[Row]

  override def initFlow() = writeData.start()

}


