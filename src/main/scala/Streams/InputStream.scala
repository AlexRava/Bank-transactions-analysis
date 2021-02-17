package Streams

import App.App.spark
import Data.DataObject.{Transaction, TransactionFactory}
import org.apache.spark.sql.Dataset
import Data.DataObject


object InputStream extends StreamingFlow {

  import spark.implicits._


  //override def readStream = transaction

  override def readStream /*Dataset[Transaction]*/ = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "input-topic")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String,String)]
    .map(_._2.split(",").toList)
    //.map(_.asInstanceOf[TransactionSerialized]) //non funziona perche lo split da in output una lista e non una tupla
    .map(TransactionFactory.createTransaction(_))
    .select($"*")

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


  override def start(): Unit = ???

  override def conf(): Unit = ???

}
