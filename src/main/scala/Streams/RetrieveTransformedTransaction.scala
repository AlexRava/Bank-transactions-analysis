package Streams
import App.App.spark
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{struct, to_json}


class RetrieveTransformedTransaction(val user: String, val transactionId: String) extends FinishedFlow {

  import spark.implicits._

  override def readData(): DataFrame = spark
    .read
    .cassandraFormat("transformed_transactions", "bank")
    .load()

  override def compute(): DataFrame = readData()
    .filter("uid = '" + user.mkString + "'") // 'where' is computed on Cassandra Server, not in spark ( ?? )
    .select($"uid" as "key", to_json(struct($"*")) as "value")

  override def writeData[DataFrameWriter[Row]](): sql.DataFrameWriter[Row] = compute()
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
    .option("topic", "transaction-transformed") // HOW TO PARTITION (?)
}
