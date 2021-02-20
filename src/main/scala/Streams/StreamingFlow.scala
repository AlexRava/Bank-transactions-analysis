package Streams

import App.App.spark
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Row, streaming}

import scala.collection.mutable

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

trait MultipleSources /*extends StreamingFlow*/{

  var dataSources: mutable.Map[String,DataFrame] = mutable.HashMap()

  def addSource(sourceName: String, dataSource: DataFrame) = this.dataSources.put(sourceName,dataSource)

  def mergeSources(sources: mutable.Map[String,DataFrame]): DataFrame

}

trait StreamingFlowWithMultipleSources extends StreamingFlow with MultipleSources


