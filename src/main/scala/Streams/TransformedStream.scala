package Streams
import java.io.File

import App.App.spark
import Data.DataObject.{Transaction, TransactionFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{from_json, struct, to_json}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.pmml4s.model.Model
import org.pmml4s.spark.ScoreModel

object TransformedStream extends StreamingFlow {
  import spark.implicits._

  override def readStream() = ???

  def write():StreamingQuery/*DataFrame*/  =  spark
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
    .writeStream
    .outputMode(OutputMode.Update)
    .foreachBatch( (batchDF: DataFrame, batchId:Long) => {
      //batchDF.collect.foreach(user => {
        val pathModel = "model\\RandomForestModel.pmml"
        val model = ScoreModel.fromFile(new File(pathModel))
        model.transform(batchDF)
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
        .option("topic", "results") // HOW TO PARTITION (?)
        .save()
      }).start()



  def modelImport() {
    val pathModel = "model\\RandomForestModel.pmml"
    val model = ScoreModel.fromFile(new File(pathModel))
  }

  override def start(): Unit = ???

  override def conf(): Unit = ???
}
