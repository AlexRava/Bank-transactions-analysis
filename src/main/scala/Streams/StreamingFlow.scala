package Streams

import App.Application.spark
import Sources.{CassandraSource, KafkaSource, Source}
import Streams.InputStream.inputSource
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Row, streaming}
import org.apache.spark.sql.cassandra._

import scala.collection.mutable

trait Flow{

  def startFlow()

}

abstract class AbstractFlow extends Flow{

  //def setSource(source : Source)
  //def readData(flowSource: Source): DataFrame
  //def readData():DataFrame
  protected def compute(): DataFrame


  //def writeData[T <: AnyRef](): AnyRef

  //def writeData [T]():T
  //def writeData[Any]():Any
  //def startFlow()

}

abstract class AbstractFinishedFlow extends AbstractFlow{

  //override type writer = org.apache.spark.sql.DataFrameWriter[Row]

  protected def writeData[DataFrameWriter[Row]](): org.apache.spark.sql.DataFrameWriter[Row]

  override def startFlow() = writeData.save()

}

abstract class AbstractStreamingFlow extends AbstractFlow{

  //type pino <: Row
  //type Transaction <: Row

 protected def writeData[DataStreamWriter[Row]](): org.apache.spark.sql.streaming.DataStreamWriter[Row]
  //protected def writeData[T](): T
  //override def startFlow() = writeData[streaming.DataStreamWriter[Row]].start()
  //override startFlow() = writeData().start()
  //override protected def compute(): DataFrame = ???

  override def startFlow(): Unit = writeData().start()
}
/*
abstract class AbstractMultipleSources extends MultipleSources {

  var dataSources: mutable.Map[String,DataFrame] = mutable.HashMap()

  override def addSource(source: Source) = this.dataSources.put(source.name, source.readFromSource())

}*/

/**
  * `Something` with multiple sources
  */
trait MultipleSources {

  //var mergeStrategy: mutable.Map[String,DataFrame] => DataFrame = _

  def addSource(source: Source)

  def setMergeStrategy(strategy: mutable.Map[String,DataFrame] => DataFrame ) /*= mergeStrategy = strategy*/

  /*def addSource(source: Source/*, dataSource: DataFrame*/) = {
    def readsource(source: Source) = source match {
      case source: KafkaSource => spark
      .readStream
      .format(source.sourceType)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", source.topic)
      .load()

      case source: CassandraSource => spark
      .read
      .cassandraFormat(source.table ,source.namespace)
      .load()
    }

    def sourceName(source:Source) = source match {}

    this.dataSources.put(source.sourceType, readsource(source))
  }*/

  //def mergeSources(sources: mutable.Map[String,DataFrame]): DataFrame

}

//trait StreamingFlowWithMultipleSources extends StreamingFlow with MultipleSources


