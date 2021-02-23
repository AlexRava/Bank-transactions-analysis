package Utility

import org.apache.spark.sql.Dataset

object StreamUtility {

  def printInStdOut [T](dataset: Dataset[T]) {
    dataset
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
