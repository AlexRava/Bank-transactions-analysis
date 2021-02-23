package Utility

import java.io.File
import java.util.Properties

import Data.{DataFactory, Transaction, TransactionTransformed, Prediction}
import SinkConnector.{CassandraSink, SinkPrediction, SinkTransaction, SinkTransformed}
import Sources.BankCassandraSource
import Utility.LoadTableInCassandra._
import com.github.tototoshi.csv.CSVReader
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.spark.sql.{ForeachWriter, Row}
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


  def readData(path : String, props: Properties)= {
    val producer: Producer[String, String] = new KafkaProducer(props)
    val reader = CSVReader.open(new File(path))
    reader.foreach(transaction => {
      producer.send(new ProducerRecord[String,String](TOPIC_NAME, transaction.mkString(",")))
    })
  }

  def loadBankTransactions(source : BankCassandraSource) = readKafkaTopic()
    .map(DataFactory.createTransaction(_))
    .writeStream
    .outputMode(OutputMode.Append)
    .foreach((new SinkTransaction(source)).asInstanceOf[ForeachWriter[Transaction]])
    .start()

  def loadSimulationTransformed(source : BankCassandraSource) = readKafkaTopic()
      .map(DataFactory.createTransactionTransformed(_))
      .writeStream
      .outputMode(OutputMode.Append)
      .foreach((new SinkTransaction(source)).asInstanceOf[ForeachWriter[TransactionTransformed]])
      .start()

  def loadSimulationPrediction(source : BankCassandraSource) = readKafkaTopic()
    .map(DataFactory.createPrediction(_))
    .writeStream
    .outputMode(OutputMode.Append)
    .foreach((new SinkPrediction(source)).asInstanceOf[ForeachWriter[Data.Prediction]])
    .start()

  /*def loadBankResult(source : BankCassandraSource) = readKafkaTopic()
    .map(DataFactory.createPrediction(_))
    .writeStream
    .outputMode(OutputMode.Append)
    .foreach(new CassandraSinkTransformed(source))
    .start()*/

}
