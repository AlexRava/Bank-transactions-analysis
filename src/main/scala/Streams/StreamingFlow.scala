package Streams

import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait StreamingFlow {

  //def readStream[T]():Dataset[T]
  def readData(): DataFrame
  def compute(): DataFrame
  def writeData(): DataStreamWriter[Row]

  def start() = writeData.start()

  /*def read()
  def write()*/
  //def conf()
  //def stream
}
