package App

import Data.DataObject.{Transaction, TransactionFactory}
import org.apache.spark.sql.functions.{struct, to_json, from_json, col}
import org.apache.spark.sql.cassandra._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.OutputMode


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

  val transaction = spark
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


  //per ogni transactions si devono mettere in un topic kafka le relative transazioni storiche dell'utente
  val retrieveAllUserTransactions = transaction
    .select($"uid")
    .writeStream
    .outputMode(OutputMode.Update)
    .foreachBatch( (batchDF: DataFrame, batchId:Long) => {
      batchDF.collect.foreach(user => spark
        .read
        .cassandraFormat("transactions1","bank")
        .load()
        .filter("uid = '" + user.mkString + "'")// 'where' is computed on Cassandra Server, not in spark ( ?? )
        .select($"uid" as "key" , to_json(struct($"*")) as "value")

        //.show()
        //.toDF("key", "value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
        .option("topic", "allTransactions") // HOW TO PARTITION (?)
        .save()
      )
    })
    .start()
  //retrieveAllUserTransactions.awaitTermination()

  //leggo lo stream che contiene tutte le transazioni di un utente, input da stream di dati in ingresso al sisteam + db
  val userTransactions = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "allTransactions")
    .load()
    .select( $"key" cast "string" as "uid", $"value" cast "string" as "json")
    .select($"uid",from_json($"json",Transaction.schema) as "data")

  //prendo tutte le transazioni di un utente e ne calcolo le nuove feature, mi sa che è opportuno fare un merge con le transazioni in ingresso
  //POSSO DECIDERE COME ELIMINARE I VALORI VECCHI PERCHE ALTRIMENTI RIMANGONO SEMPRE TUTTI


  val feature1 = userTransactions

    // mi sa che prima faccio tutte le selezioni di dati e manipolazioni e poi alla fine faccio il group by con l'operazione di aggrgazione tipo: count
    .groupBy($"uid")
    .count
    .map(_)

  val feature2 = userTransactions

  





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

