package Streams

import App.App.spark
import Data.DataObject.Transaction
import org.apache.spark.sql.functions.{col, from_json, window}
import org.apache.spark.sql.functions._


object allTransactions extends StreamingFlow{

  import spark.implicits._

  val read =  spark
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


  //val write = manipolazione, in realtà prendo input da csv

  /*
    //CI SARà UN PUNTO DI JOIN TRA IL RISULTATI DI FEATURE1, FEATURE2, FEATUREn E CON IL FLUSSO DI DATI in arrivo dall'utente ( per avere tutti i valori)
  //  forse il join è prima considerato che alcuni valori della transazione servono per calcolar le feature
  // il join della transazione in arrivo potrebbe essere con user transactions ( prima della divisione dei diversi stream)
  // join o semplice invio nello stesso topic ?? come sitringuo la transazione targe dalle altre transazioni ? time stamp? è la trans.
  //con time stamp più recente altrimenti..


  def myWindow(n: Int) ={
    def days(n:Int) = 60*60*24*n
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
  val numberofTransactionsLastPeriod = read
  .groupBy(window(col("data.transactiondt"),"1 week")).agg(sum("data.transactionamt")) //non è il tipo di window che mi interessa
  //ok funzionava bene se fosse stato possibile lavorare su finestre non strettamente temporali
  //.withColumn("newColSomma", sum(col("data.transactionamt")).over(weekWindow)).orderBy(asc("data.transactiondt"))
  */

  override def start(): Unit = ???

  override def conf(): Unit = ???
}
