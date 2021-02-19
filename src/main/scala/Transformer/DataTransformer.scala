package Transformer

import App.App.spark
import Data.DataObject.Transaction
import Streams.StreamingFlow
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery}
import org.apache.spark.sql.cassandra._

import scala.collection.mutable

trait Transformer{
  def addSource(dataSource: DataFrame)
  //def compute():StreamingQuery
}

class DataTransformer() extends Transformer with StreamingFlow {

  import spark.implicits._


  var dataSources = mutable.Set[DataFrame]()

  //override def setSource(dataSource: StreamingFlow) = this.dataSource = dataSource
  //override def setSource(dataSource: StreamingFlow*): Unit = this.dataSource. = dataSource

  override def addSource(dataSource: DataFrame) = this.dataSources.add(dataSource)

  //deve essere incapusulato da qualche parte come mia strategia "non va bene prendere la testa, devo prendere l'input stream
  private def mergeStream() = dataSources.head

  override def readData(): DataFrame = mergeStream()

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

  implicit class DataFrameCustomOperation(base: DataFrame){

    def customOperation(): DataFrame ={
      //your business logic
      base
    }

    //def op1:Dataframe =
  }
}
