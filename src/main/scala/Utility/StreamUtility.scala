package Utility

import App.Application.spark
import org.apache.spark.sql.Dataset

object StreamUtility {

  import spark.implicits._


  def printInStdOut [T](dataset: Dataset[T]) {
    dataset
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String,String)]
      .writeStream
      .outputMode("update")
      .format("console")
      .start()
  }

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
