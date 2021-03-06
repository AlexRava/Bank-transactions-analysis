package Streams
import App.Application.spark
import Sources.{BankCassandraSource, KafkaSource, Source}
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import org.apache.spark.sql.cassandra._


class RetrieveAllTransactionsOf(val user: String, val dBSource: BankCassandraSource, val outputSource: KafkaSource) extends AbstractFinishedFlow {

  import spark.implicits._

  override protected def compute(): DataFrame = dBSource
    .readFromSource()
    .filter("uid = '" + this.user + "'")
    .select($"uid" as "key" , to_json(struct($"*")) as "value")


  override protected def writeData[DataFrameWriter[Row]](): org.apache.spark.sql.DataFrameWriter[Row] = compute()
    .write
    .format(outputSource.sourceType)
    .option("kafka.bootstrap.servers", outputSource.server)
    .option("topic", outputSource.topic)
  }


