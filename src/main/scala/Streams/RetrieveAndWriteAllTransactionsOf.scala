package Streams
import App.Application.spark
import Sources.{BankCassandraSource, KafkaSource, Source}
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import org.apache.spark.sql.cassandra._


class RetrieveAndWriteAllTransactionsOf(val user: String, val dBSource: BankCassandraSource, val outputSource: KafkaSource) extends FinishedFlow {

  import spark.implicits._

  /*override def readData(): DataFrame = dBSource.readFromSource()/* spark
    .read
    .cassandraFormat(dBSource.table ,dBSource.namespace)
    .load()*/
    .filter("uid = '" + this.user + "'")// 'where' is computed on Cassandra Server, not in spark ( ?? )
*/

  override def compute(): DataFrame = dBSource.readFromSource()
    .filter("uid = '" + this.user + "'")
    .select($"uid" as "key" , to_json(struct($"*")) as "value")


  override def writeData[DataFrameWriter[Row]](): org.apache.spark.sql.DataFrameWriter[Row] = compute()
    .write
    .format(outputSource.sourceType)
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
    .option("topic", outputSource.topic)
  }


