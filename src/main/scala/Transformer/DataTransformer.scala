package Transformer

import App.App.spark
import Data.DataObject.Transaction
import Streams.StreamingFlow
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.cassandra._

import scala.collection.mutable

trait Transformer{
  def addSource(dataSource: StreamingFlow)
  def compute():StreamingQuery
}

class DataTransformer() extends Transformer {

  import spark.implicits._


  var dataSources = mutable.Set[StreamingFlow]()

  //override def setSource(dataSource: StreamingFlow) = this.dataSource = dataSource
  //override def setSource(dataSource: StreamingFlow*): Unit = this.dataSource. = dataSource

  override def addSource(dataSource: StreamingFlow) = this.dataSources.add(dataSource)

  //deve essere incapusulato da qualche parte come mia strategia "non va bene prendere la testa, devo prendere l'input stream
  private def mergeStream() = dataSources.head.readStream()

  override def compute( ) = {
    mergeStream()
      //forse sarebbe meglio incapsulare la strategia da qualche parte, es. arriva come parametro
      .select($"uid")
      .writeStream
      .outputMode(OutputMode.Update)
      .foreachBatch( (batchDF: DataFrame, batchId:Long) => {
        batchDF.collect.foreach(user => spark
          .read
          .cassandraFormat("transformed_transactions","bank")
          .load()
          .filter("uid = '" + user.mkString + "'")// 'where' is computed on Cassandra Server, not in spark ( ?? )
          .select($"uid" as "key" , to_json(struct($"*")) as "value")
          //.show()
          //.toDF("key", "value")
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
          .option("topic", "transaction-transformed") // HOW TO PARTITION (?)
          .save()
        )
      }).start()
  }
}
