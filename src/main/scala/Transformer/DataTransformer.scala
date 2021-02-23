package Transformer

import App.Application.spark
import Sources.CassandraSources.DbTransformed
import Sources.KafkaSources.TransactionTransformedSource
import Sources.{KafkaSource, Source}
import Streams.{AbstractFlow, AbstractStreamingFlow, MultipleSources, RetrieveTransformedTransaction, StreamingFlow, StreamingFlowWithMultipleSources}
import org.apache.spark.sql.{DataFrame, Row, streaming}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery}
import Utility.MergeStrategy
import org.apache.spark.sql.cassandra._

import scala.collection.mutable

//class DataTransformer(var outputSource: KafkaSource) extends StreamingFlowWithMultipleSources/*extends Transformer with StreamingFlow*/ {
object DataTransformer extends AbstractStreamingFlow with MultipleSources{

  import spark.implicits._
  import Utility.DataFrameExtension.ImplicitsDataFrameCustomOperation

  var sources: mutable.Map[String,DataFrame] = mutable.HashMap()
  var outputSource: KafkaSource = TransactionTransformedSource
  var mergeStrategy: mutable.Map[String,DataFrame] => DataFrame = _

  override def addSource(source: Source) = sources.put(source.name,source.readFromSource())

  override def setMergeStrategy(strategy: (mutable.Map[String,DataFrame] => DataFrame) ) = mergeStrategy = strategy

  //private def mergeSources(): DataFrame = mergeStrategy(sources)


  // this.dataSources.get("INPUT_DATA").get
  //var dataSources: mutable.Map[String,DataFrame] = mutable.HashMap()
  //override def setSource(dataSource: StreamingFlow) = this.dataSource = dataSource
  //override def setSource(dataSource: StreamingFlow*): Unit = this.dataSource. = dataSource
  //override def addSource(sourceName: String, dataSource: DataFrame) = this.dataSources.put(sourceName,dataSource)
  //deve essere incapusulato da qualche parte come mia strategia "non va bene prendere la testa, devo prendere l'input stream
  //private def mergeStream(mergeStrategy) = dataSources.merge(merge)get()

  //override def readData() = mergeSources(this.dataSources)

  override protected def compute() = mergeStrategy(sources)
    .addColHabitualBehaviour()


  override protected def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] = {
    val retrieveTrasformedDataFromDb =
      (batchDF: DataFrame, batchId: Long) => batchDF.collect.foreach(
        user => new RetrieveTransformedTransaction(user.mkString, "ID", DbTransformed, outputSource).startFlow()) // ADD TRANSACTION ID

    compute
      .select($"uid") // ADD TRANSACTION ID
      .writeStream
      .outputMode(OutputMode.Update)
      .foreachBatch(retrieveTrasformedDataFromDb)
  }
}
