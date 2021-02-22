package Streams
import App.Application.spark
import Sources.{BankCassandraSource, KafkaSource, SimulationCassandraSource}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{struct, to_json}


class RetrieveTransformedTransaction(val user: String,
                                     val transactionId: String,
                                     val dBSource: SimulationCassandraSource,
                                     val outputSource: KafkaSource) extends FinishedFlow {

  import spark.implicits._

  /*override def readData(): DataFrame = spark
    .read
    .cassandraFormat(dBSource.table, dBSource.namespace)
    .load()*/

  override def compute(): DataFrame = dBSource.readFromSource()
    .filter("uid = '" + user.mkString + "'") // 'where' is computed on Cassandra Server, not in spark ( ?? )
    .select($"uid" as "key", to_json(struct($"*")) as "value")

  override def writeData[DataFrameWriter[Row]](): sql.DataFrameWriter[Row] = compute()
    .write
    .format(outputSource.sourceType)
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
    .option("topic", outputSource.topic) // HOW TO PARTITION (?)
}
