package Streams
import App.Application.spark
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Row, streaming}

object Predict extends StreamingFlow {

  import spark.implicits._

  override def readData(): DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transaction-transformed")
    .load()


  override def compute(): DataFrame = readData()


  override def writeData[DataStreamWriter[Row]](): streaming.DataStreamWriter[Row] = compute
    //forse sarebbe meglio incapsulare la strategia da qualche parte, es. arriva come parametro
    .select($"uid") // ADD TRANSACTION ID
    .writeStream
    .outputMode(OutputMode.Update)
    .foreachBatch( retrievePrediction )

  val retrievePrediction =
    (batchDF: DataFrame, batchId: Long) => batchDF.collect.foreach(user => new RetrievePrediction(user.mkString, "ID").startFlow()) // ADD TRANSACTION ID



}
