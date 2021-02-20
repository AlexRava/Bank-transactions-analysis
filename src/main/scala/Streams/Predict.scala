package Streams
import org.apache.spark.sql.{DataFrame, Row, streaming}

object Predict extends StreamingFlow {

  override def readData(): DataFrame = {
    
  }

  override def compute(): DataFrame = ???

  override def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] = ???
}
