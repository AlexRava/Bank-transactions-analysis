package Streams
import App.App.spark
import Data.DataObject.{Transaction, TransactionFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.from_json

object TransformedStream extends StreamingFlow {
  import spark.implicits._

  override def readStream(): DataFrame =  spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "transaction-transformed")
      .load()
      .select( $"key" cast "string" as "uid", $"value" cast "string" as "json")
    //.select($"uid" , $"json".as[Transaction])
      .select($"uid",from_json($"json",Transaction.TransactionTransformedSchema) as "data")
      /*.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String,String)]
      .map(_._2.split(",").toList)*/
      //.map(_.asInstanceOf[TransactionSerialized]) //non funziona perche lo split da in output una lista e non una tupla
      //.map(TransactionFactory.createTransactionTransformed(_))
      .select($"*")



  override def start(): Unit = ???

  override def conf(): Unit = ???
}
