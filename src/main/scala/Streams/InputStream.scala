package Streams

import App.App.spark
import Data.DataObject.Transaction
import org.apache.spark.sql.Dataset

object InputStream extends StreamingFlow {

  import spark.implicits._

  val transaction: Dataset[Transaction] = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "input-topic")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String,String)]
    .map(_._2.split(",").toList)
    //.map(_.asInstanceOf[TransactionSerialized]) //non funziona perche lo split da in output una lista e non una tupla
    .map(Transaction(_))
  //.map(_.uid)
  //.filter(t => (t.TransactionID != "") & (t.uid != ""))
  //.map(_.uid)
  //.toDF()
  def transactions = transaction

  override def start(): Unit = ???

  override def conf(): Unit = ???
}
