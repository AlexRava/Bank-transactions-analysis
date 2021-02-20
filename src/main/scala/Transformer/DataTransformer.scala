package Transformer

import App.Application.spark
import Data.DataObject.Transaction
import Streams.{RetrieveTransformedTransaction, StreamingFlow, StreamingFlowWithMultipleSources}
import org.apache.spark.sql.{DataFrame, Row, streaming}
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery}
import Utility.DataFrameOperation

import scala.collection.mutable

trait Transformer{
  def addSource(sourceName: String, dataSource: DataFrame)
  //def compute():StreamingQuery
}

class DataTransformer extends StreamingFlowWithMultipleSources/*extends Transformer with StreamingFlow*/ {

  import spark.implicits._
  import DataFrameOperation.ImplicitsDataFrameCustomOperation

  override def mergeSources(sources: mutable.Map[String,DataFrame]): DataFrame = this.dataSources.get("INPUT_DATA").get

  //var dataSources: mutable.Map[String,DataFrame] = mutable.HashMap()
  //override def setSource(dataSource: StreamingFlow) = this.dataSource = dataSource
  //override def setSource(dataSource: StreamingFlow*): Unit = this.dataSource. = dataSource

  //override def addSource(sourceName: String, dataSource: DataFrame) = this.dataSources.put(sourceName,dataSource)

  //deve essere incapusulato da qualche parte come mia strategia "non va bene prendere la testa, devo prendere l'input stream
  //private def mergeStream(mergeStrategy) = dataSources.merge(merge)get()

  override def readData(): DataFrame = mergeSources(this.dataSources)

  override def compute() = readData()
    .customOperation()


  override def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] = compute
    //forse sarebbe meglio incapsulare la strategia da qualche parte, es. arriva come parametro
    .select($"uid") // ADD TRANSACTION ID
    .writeStream
    .outputMode(OutputMode.Update)
    .foreachBatch( retrieveTrasformedDataFromDb )

  val retrieveTrasformedDataFromDb =
    (batchDF: DataFrame, batchId: Long) => batchDF.collect.foreach(user => new RetrieveTransformedTransaction(user.mkString, "ID").startFlow()) // ADD TRANSACTION ID


}
