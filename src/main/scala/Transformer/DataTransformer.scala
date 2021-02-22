package Transformer

import App.Application.spark
import Sources.CassandraSources.DbTransformedSource
import Sources.{KafkaSource, Source}
import Streams.{RetrieveTransformedTransaction, StreamingFlow, StreamingFlowWithMultipleSources}
import org.apache.spark.sql.{DataFrame, Row, streaming}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery}
import Utility.{DataFrameOperation, MergeStrategy}
import org.apache.spark.sql.cassandra._


import scala.collection.mutable

trait Transformer{
  def addSource(sourceName: String, dataSource: DataFrame)
}

class DataTransformer(var outputSource: KafkaSource) extends StreamingFlowWithMultipleSources/*extends Transformer with StreamingFlow*/ {

  import spark.implicits._
  import DataFrameOperation.ImplicitsDataFrameCustomOperation

  var mergeStrategy : Map[String,DataFrame] => DataFrame
  var sources: Map[String,DataFrame]

  def setMergeStrategy(strategy: (Map[String,DataFrame] => DataFrame) ) = this.mergeStrategy = strategy

  override def addSource(sourceName: Source)//, dataSource: DataFrame): Option[DataFrame] = super.addSource(sourceName, dataSource)

  def mergeSources(): DataFrame = this.mergeStrategy(sources)



  // this.dataSources.get("INPUT_DATA").get
  //var dataSources: mutable.Map[String,DataFrame] = mutable.HashMap()
  //override def setSource(dataSource: StreamingFlow) = this.dataSource = dataSource
  //override def setSource(dataSource: StreamingFlow*): Unit = this.dataSource. = dataSource
  //override def addSource(sourceName: String, dataSource: DataFrame) = this.dataSources.put(sourceName,dataSource)
  //deve essere incapusulato da qualche parte come mia strategia "non va bene prendere la testa, devo prendere l'input stream
  //private def mergeStream(mergeStrategy) = dataSources.merge(merge)get()

  override def readData() = mergeSources(this.dataSources)

  override def compute() = readData()
    .customOperation()


  override def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] = {
    val retrieveTrasformedDataFromDb =
      (batchDF: DataFrame, batchId: Long) => batchDF.collect.foreach(
        user => new RetrieveTransformedTransaction(user.mkString, "ID", DbTransformedSource, outputSource).startFlow()) // ADD TRANSACTION ID

    compute
      .select($"uid") // ADD TRANSACTION ID
      .writeStream
      .outputMode(OutputMode.Update)
      .foreachBatch(retrieveTrasformedDataFromDb)
  }

  override def mergeSources(sources: mutable.Map[String, DataFrame]): DataFrame = ???
}
