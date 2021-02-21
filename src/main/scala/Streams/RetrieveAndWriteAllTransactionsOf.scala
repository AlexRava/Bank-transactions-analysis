package Streams
import App.Application.spark
import Sources.Source
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.cassandra._


class RetrieveAndWriteAllTransactionsOf(val user: String, val outputSource: Source) extends FinishedFlow {

  import spark.implicits._

  // la tabella dove a a prendere le transazioni è embbeddata (?)

  override def readData(): DataFrame = spark
    .read
    .cassandraFormat("transactions1","bank")
    .load()
    .filter("uid = '" + this.user + "'")// 'where' is computed on Cassandra Server, not in spark ( ?? )


  override def compute(): DataFrame = readData
    .select($"uid" as "key" , to_json(struct($"*")) as "value")


  override def writeData[DataFrameWriter[Row]](): org.apache.spark.sql.DataFrameWriter[Row] = compute()
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
    .option("topic", outputSource.topicSource)
  }


