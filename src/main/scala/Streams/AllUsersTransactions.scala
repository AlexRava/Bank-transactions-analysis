package Streams

import App.App.{spark, transaction}
import Data.DataObject.Transaction
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.cassandra._

object AllUsersTransactions extends StreamingFlow {

  import spark.implicits._

  def allUserTransactions(userTransaction : DataFrame) =
    /*userTransaction
    .select($"uid")
    .writeStream
    .outputMode(OutputMode.Update)
    .foreachBatch( (batchDF: DataFrame, batchId:Long) => {
      batchDF.collect.foreach(user => spark
        .read
        .cassandraFormat("transactions1","bank")
        .load()
        .filter("uid = '" + user.mkString + "'")// 'where' is computed on Cassandra Server, not in spark ( ?? )
        .select($"uid" as "key" , to_json(struct($"*")) as "value")
        //.foreach(row => println(row))

        //.show()
        //.toDF("key", "value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
        .option("topic", "allTransactions") // HOW TO PARTITION (?)
        .save()
      )
    })*/
  override def readStream(): DataFrame = ???



  override def start(): Unit = ???

  override def conf(): Unit = ???

}
