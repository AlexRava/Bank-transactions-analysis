package Streams
import App.Application.spark
import Sources.KafkaSource
import Sources.KafkaSources.{AllTransactionSource, InputSource, PredictionSource, TransactionTransformedSource}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Row, streaming}

object Predict extends AbstractStreamingFlow {

  val inputSource: KafkaSource = TransactionTransformedSource
  val outputSource: KafkaSource = PredictionSource

  import spark.implicits._
  import Utility.DataFrameExtension._

  /*override def readData(): DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transaction-transformed")
    .load()
*/

  override protected def compute(): DataFrame =  TransactionTransformedSource
    .readFromSource()
    .addPrediction()

  override protected def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] = {
    val retrievePrediction = (batchDF: DataFrame, batchId: Long) =>
      batchDF.collect.foreach(user =>  new RetrievePrediction(user.mkString, "ID").startFlow()) // ADD TRANSACTION ID

    compute
      .select($"uid") // select($"uid",$"transactionID",$"prediction")
      .writeStream
      .outputMode(OutputMode.Update)
      .foreachBatch( retrievePrediction )
  }




}
