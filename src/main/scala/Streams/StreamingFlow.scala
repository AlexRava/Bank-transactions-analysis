package Streams

import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Row, streaming}

trait Flow {

  def readData(): DataFrame
  def compute(): DataFrame
  //def writeData [T]():T
  def writeData[T <: Any](): Any
  //def writeData[T <: Dataset[Row] ](): Dataset[Row]

  def initFlow()

}

trait FinishedFlow extends Flow{

  override def writeData[DataFrameWriter[Row]](): DataFrameWriter[Row]

  override def initFlow() = writeData.asInstanceOf[DataFrameWriter[Row]].save()


}

trait StreamingFlow extends Flow{

  override def writeData[DataStreamWriter[Row]]():DataStreamWriter[Row]

  override def initFlow() = writeData.asInstanceOf[DataStreamWriter[Row]].start()


  /*def read()
  def write()*/
  //def conf()
  //def stream
}


