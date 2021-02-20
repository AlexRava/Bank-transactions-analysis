package Streams

import App.Application.spark
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Row, streaming}

import scala.collection.mutable

trait Flow {

  def readData(): DataFrame
  def compute(): DataFrame
  //def writeData[T <: AnyRef](): AnyRef

  //def writeData [T]():T
  //def writeData[Any]():Any

  def startFlow()

}

trait FinishedFlow extends Flow{

  def writeData[DataFrameWriter[Row]](): org.apache.spark.sql.DataFrameWriter[Row]

  override def startFlow() = writeData.save()


}

trait StreamingFlow extends Flow{

  def writeData[DataStreamWriter[Row]](): org.apache.spark.sql.streaming.DataStreamWriter[Row]

  override def startFlow() = writeData.start()

}

trait MultipleSources /*extends StreamingFlow*/{

  var dataSources: mutable.Map[String,DataFrame] = mutable.HashMap()

  def addSource(sourceName: String, dataSource: DataFrame) = this.dataSources.put(sourceName,dataSource)

  def mergeSources(sources: mutable.Map[String,DataFrame]): DataFrame

}

trait StreamingFlowWithMultipleSources extends StreamingFlow with MultipleSources


