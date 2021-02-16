package App

import Data.DataObject.{Transaction, TransactionFactory}
import Streams.{AllUsersTransactions, InputStream}
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.cassandra._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.apache.spark.sql.functions._

object App extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf: SparkConf = new SparkConf()
    .setAppName("structured-streaming-kafka-hello-world")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.sql.streaming.checkpointLocation", "checkpoint")

  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val transaction = InputStream.transaction
  /*val transaction = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "input-topic")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String,String)]
    .map(_._2.split(",").toList)
    //.map(_.asInstanceOf[TransactionSerialized]) //non funziona perche lo split da in output una lista e non una tupla
    .map(Transaction(_))*/

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

  val allUsersTransactions : DataStreamWriter[Row] = AllUsersTransactions(transaction)
  allUsersTransactions.start()
  //per ogni transactions si devono mettere in un topic kafka le relative transazioni storiche dell'utente

  //retrieveAllUserTransactions.awaitTermination()


  //leggo lo stream che contiene tutte le transazioni di un utente, input da stream di dati in ingresso al sisteam + db
  val userTransactions = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "allTransactions")
    .load()
    .select( $"key" cast "string" as "uid", $"value" cast "string" as "json")
    //.select($"uid" , $"json".as[Transaction])

  .select($"uid",from_json($"json",Transaction.schema) as "data")

  //.select($"data").as[Transaction]
    //.map(row => (row.fieldIndex("uid"),Transaction(row.toString.split(",").toList))) // non so se è proprio quello che voglio



  //prendo tutte le transazioni di un utente e ne calcolo le nuove feature, mi sa che è opportuno fare un merge con le transazioni in ingresso
  //POSSO DECIDERE COME ELIMINARE I VALORI VECCHI PERCHE ALTRIMENTI RIMANGONO SEMPRE TUTTI

    def myWindow(n: Int) ={
      def days(n:Int) = 60*60*24*n

        window(col("data.transactiondt"),"1 week")
        //.agg(avg("Close").as("weekly_average"))

      //windows not supported, peccato, sarebbero state perfette
      /*Window
        .partitionBy($"uid")
        .orderBy(col("data.transactiondt").cast("timestamp"))//.cast("long"))
        .rangeBetween(-days(n), Window.currentRow)*/
    }

  def weekWindow = myWindow(7)
  def monthWindow =  myWindow(30)

  //numero di transazioni nell'ultimo mese minori di range1
  val range1: Double = 80
  val numberofTransactionsLastPeriod = userTransactions
    //.groupBy(window(col("data.transactiondt"),"1 week",))
    //.agg(sum("data.transactionamt"))

  //ok funzionava bene se fosse stato ok
  //.withColumn("newcol", sum(col("data.transactionamt")).over(weekWindow)).orderBy(asc("data.transactiondt"))


  val printUid = numberofTransactionsLastPeriod//userTransactions
        .writeStream
        .outputMode("update")
        .format("console")
        .start()


    // mi sa che prima faccio tutte le selezioni di dati e manipolazioni e poi alla fine faccio il group by con l'operazione di aggrgazione tipo: count
    //.groupBy($"uid")
    //.count
    //.map(_)*/

  val feature2 = userTransactions

  val featureN = userTransactions

  //CI SARà UN PUNTO DI JOIN TRA IL RISULTATI DI FEATURE1, FEATURE2, FEATUREn E CON IL FLUSSO DI DATI in arrivo dall'utente ( per avere tutti i valori)
  //  forse il join è prima considerato che alcuni valori della transazione servono per calcolar le feature
  // il join della transazione in arrivo potrebbe essere con user transactions ( prima della divisione dei diversi stream)
  // join o semplice invio nello stesso topic ?? come sitringuo la transazione targe dalle altre transazioni ? time stamp? è la trans.
  //con time stamp più recente altrimenti..



  spark.streams.awaitAnyTermination()

  //WRITE in Cassandra DB
  /*val writeQueryCassandra = transactions
    .writeStream
    .outputMode("update")
    .foreach(new CassandraSink())
    .start()
  writeQueryCassandra.awaitTermination()*/

   //PRINT in std out
  /*val  printUid = transactions
    .writeStream
    .outputMode("update")
    .format("console")
    .start()
  printUid.awaitTermination()*/
}

