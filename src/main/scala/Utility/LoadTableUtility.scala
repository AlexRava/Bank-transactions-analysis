package Utility

import Data.{DataFactory, Transaction, TransactionTransformed}
import SinkConnector.{SinkPrediction, SinkTransaction, SinkTransformed}
import Sources.CassandraSource
import java.io.File
import Utility.LoadTableInCassandra._
import com.github.tototoshi.csv.CSVReader
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.spark.sql.streaming.OutputMode

object LoadTableUtility {

  import spark.implicits._

  private def readKafkaTopic() = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", TOPIC_NAME)
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String,String)]
    .map(_._2.split(",").toList)


  def readData(path : String)= {
    val producer: Producer[String, String] = new KafkaProducer(props)
    val reader = CSVReader.open(new File(path))
    reader.foreach(transaction => { Thread.sleep(400); producer.send(new ProducerRecord[String,String](TOPIC_NAME, transaction.mkString(",")));})
  }

  def loadBankTransactions(source : CassandraSource) = readKafkaTopic()
    .map(DataFactory.createTransaction(_))
    .writeStream
    .outputMode(OutputMode.Append)
    .foreach(new SinkTransaction(source))
    .start()

  def loadSimulationTransformed(source : CassandraSource) = readKafkaTopic()
      .map(DataFactory.createTransactionTransformed(_))
      .writeStream
      .outputMode(OutputMode.Append)
      .foreach(new SinkTransformed(source))
      .start()

  def loadSimulationPrediction(source : CassandraSource) = readKafkaTopic()
    .map(DataFactory.createPrediction(_))
    .writeStream
    .outputMode(OutputMode.Append)
    .foreach(new SinkPrediction(source))
    .start()
}
