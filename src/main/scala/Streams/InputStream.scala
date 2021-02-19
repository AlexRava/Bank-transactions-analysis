package Streams

import App.App.spark
import Data.DataObject.{Transaction, TransactionFactory}
import org.apache.spark.sql.{DataFrame, Dataset}
import Data.DataObject
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.cassandra._


object InputStream extends StreamingFlow {

  import spark.implicits._

  override def readData() /*Dataset[Transaction]*/ = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "input-topic")
    .load()

  override def compute() =
    readData
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String,String)]
    .map(_._2.split(",").toList)
    //.map(_.asInstanceOf[TransactionSerialized]) //non funziona perche lo split da in output una lista e non una tupla
    .map(TransactionFactory.createTransaction(_))
    .select($"*")
    .select($"uid")

  override def writeData =
    compute
    .writeStream
    .outputMode(OutputMode.Update)
    .foreachBatch( retrieveDataforEachUsersInBatch )

  //override def start() = writeData.start()



  val retrieveDataforEachUsersInBatch = (batchDF: DataFrame, batchId:Long) => {
      batchDF.collect.foreach(user => spark
        .read
        .cassandraFormat("transactions1","bank")
        .load()
        .filter("uid = '" + user.mkString + "'")// 'where' is computed on Cassandra Server, not in spark ( ?? )
        .select($"uid" as "key" , to_json(struct($"*")) as "value")
        //.foreach(row => println(row))
        //.show()
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
        .option("topic", "allTransactions") // HOW TO PARTITION (?)
        .save()
      )
    }


  //.map(_.uid)
  //.filter(t => (t.TransactionID != "") & (t.uid != ""))
  //.map(_.uid)
  //.toDF()

  //def transactions = transaction

  //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  //stream that publish the transactions on the topic where will be computed the feature engineering phase
  /*val loadTransaction = transaction
    .select($"uid", to_json(struct($"*")))
    .toDF("key", "value")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
    .option("topic", "allTransactions")
    .save()*/

  //send all transactions to the feature eng topic




  override def conf(): Unit = ???


}
