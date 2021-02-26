package Streams

import Sources.Source
import org.apache.spark.sql.{DataFrame, Row }


import scala.collection.mutable

trait Flow{

  def startFlow()

}

abstract class AbstractFlow extends Flow{

  protected def compute(): DataFrame

}

abstract class AbstractFinishedFlow extends AbstractFlow{

  protected def writeData[DataFrameWriter[Row]](): org.apache.spark.sql.DataFrameWriter[Row]

  override def startFlow() = writeData.save()

}

abstract class AbstractStreamingFlow extends AbstractFlow{

 protected def writeData[DataStreamWriter[Row]](): org.apache.spark.sql.streaming.DataStreamWriter[Row]

  override def startFlow(): Unit = writeData().start()
}

/**
  * Something modelld with multiple sources
  */
trait MultipleSources {

  def addSource(source: Source)

  def setMergeStrategy(strategy: mutable.Map[String,DataFrame] => DataFrame )

}


