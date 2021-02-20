/*
package Streams

import java.io.File

import App.Application.spark
import Data.DataObject.{Transaction, TransactionFactory}
import org.apache.spark.sql.{DataFrame, Row, streaming}
import org.apache.spark.sql.functions.{from_json, struct, to_json}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.pmml4s.spark.ScoreModel

object TransformedStream extends StreamingFlow {
  import spark.implicits._

  //override def readStream() = ???

  def write():/*StreamingQuery*/DataFrame  =  spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "transaction-transformed")
      .load()
      .select( $"key" cast "string" as "uid", $"value" cast "string" as "json")
      //.select($"uid",from_json($"json",Transaction.TransactionTransformedSchema) as "data")
      .select($"*")




  //.select($"uid" , $"json".as[Transaction])
    /*.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String,String)]
      .map(_._2.split(",").toList)*/
      //.map(_.asInstanceOf[TransactionSerialized]) //non funziona perche lo split da in output una lista e non una tupla
      //.map(TransactionFactory.createTransactionTransformed(_))
    /*.writeStream
    .outputMode(OutputMode.Update)
    .foreachBatch( (batchDF: DataFrame, batchId:Long) => {
      //batchDF.collect.foreach(transaction => {
        val pathModel = "model\\RandomForestModel.pmml"
        val model = ScoreModel.fromFile(new File(pathModel))
      //println("sssssss")
        batchDF
        //model
        //.transform(batchDF)
        .select($"data")
        .foreach(row => println(row))
          //.show()
*/




      /*
        //.transform(transaction)
        .select($"uid" as "key" , to_json(struct($"*")) as "value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
        .option("topic", "results") // HOW TO PARTITION (?)
        .save()*/

     // })//.start()



  def modelImport() {
    val pathModel = "model\\RandomForestModel.pmml"
    val model = ScoreModel.fromFile(new File(pathModel))
  }

  //override def start(): Unit = ???

  //override def conf(): Unit = ???
  override def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] = ???

  override def readData(): DataFrame = ???

  override def compute(): DataFrame = ???
}
*/