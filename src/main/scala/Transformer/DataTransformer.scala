package Transformer

import App.App.spark
import Data.DataObject.Transaction
import Streams.{MultipleSource, StreamingFlow, StreamingFlowWithMultipleSources}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery}
import org.apache.spark.sql.cassandra._
import Utility.DataFrameOperation

import scala.collection.mutable

trait Transformer{
  def addSource(sourceName: String, dataSource: DataFrame)
  //def compute():StreamingQuery
}

class DataTransformer() extends StreamingFlowWithMultipleSources/*extends Transformer with StreamingFlow*/ {

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



  override def writeData(): DataStreamWriter[Row] =
    //forse sarebbe meglio incapsulare la strategia da qualche parte, es. arriva come parametro
    compute
    .select($"uid")
    .writeStream
    .outputMode(OutputMode.Update)
    .foreachBatch( retrieveTrasformedDataFromDb )

  val retrieveTrasformedDataFromDb = (batchDF: DataFrame, batchId: Long) => {
    batchDF.collect.foreach(user => spark
      .read
      .cassandraFormat("transformed_transactions", "bank")
      .load()
      .filter("uid = '" + user.mkString + "'") // 'where' is computed on Cassandra Server, not in spark ( ?? )
      .select($"uid" as "key", to_json(struct($"*")) as "value")
        //.show()
        //.toDF("key", "value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
      .option("topic", "transaction-transformed") // HOW TO PARTITION (?)
        save()
    )
  }



    //def op1:Dataframe =
  }
}
