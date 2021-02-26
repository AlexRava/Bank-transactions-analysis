package Streams

import App.Application.spark
import Data.DataFactory
import Sources.CassandraSources.DbTransformed
import Sources.KafkaSources.TransactionTransformedSource
import Sources.{KafkaSource, Source}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Row, streaming}
import scala.collection.mutable

object DataTransformer extends AbstractStreamingFlow with MultipleSources{

  import Utility.DataFrameExtension.ImplicitsDataFrameCustomOperation
  import spark.implicits._

  var sources: mutable.Map[String,DataFrame] = mutable.HashMap()
  val outputSource: KafkaSource = TransactionTransformedSource
  private var mergeStrategy: mutable.Map[String,DataFrame] => DataFrame = _

  override def addSource(source: Source) = sources.put(source.name,source.readFromSource())

  override def setMergeStrategy(strategy: (mutable.Map[String,DataFrame] => DataFrame) ) = mergeStrategy = strategy

  override protected def compute() = mergeStrategy(sources)
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String,String)]
    .map(_._2.split(",").toList)
    .map(DataFactory.createTransaction(_))
    .select("*")
    .addColHabitualBehaviour()

  override protected def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] = {
    val retrieveTrasformedDataFromDb =
      (batchDF: DataFrame, batchId: Long) =>
        batchDF.collect.foreach( user =>
          new RetrieveSimulatedData(s"${user(0)}", s"${user(1)}", DbTransformed, outputSource).startFlow())

    compute
      .select($"uid",$"TransactionID")
      .writeStream
      .outputMode(OutputMode.Update)
      .foreachBatch(retrieveTrasformedDataFromDb)
  }
}
