package Streams
import App.Application.spark
import org.apache.spark.sql
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.cassandra._


class RetrievePrediction(val user: String, val transactionId: String) extends AbstractFinishedFlow {

  import spark.implicits._

  def readData(): DataFrame = spark
    .read
    .cassandraFormat("prediction", "bank")
    .load()

  override protected def compute(): DataFrame = readData()
    .filter("uid = '" + user + "'") // 'where' is computed on Cassandra Server, not in spark ( ?? )
    .select($"uid" as "key", to_json(struct($"*")) as "value")

  override protected def writeData[DataFrameWriter[Row]](): sql.DataFrameWriter[Row] = compute()
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
    .option("topic", "results") // HOW TO PARTITION (?)

}
