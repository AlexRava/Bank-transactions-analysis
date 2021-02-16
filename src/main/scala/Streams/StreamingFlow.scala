package Streams

import org.apache.spark.sql.{DataFrame, Dataset}

trait StreamingFlow {

  //def readStream[T]():Dataset[T]
  def readStream(): DataFrame

  /*def read()
  def write()*/

  def start()
  def conf()
  //def stream
}
