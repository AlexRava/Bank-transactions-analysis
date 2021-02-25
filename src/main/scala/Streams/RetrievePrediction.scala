package Streams
import App.Application.spark
import Sources.{KafkaSource, SimulationCassandraSource}
import org.apache.spark.sql
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.cassandra._

class RetrievePrediction(val user: String,
                         val transactionId: String,
                         val dBSource: SimulationCassandraSource,
                         val outputSource: KafkaSource) extends AbstractFinishedFlow {

  import spark.implicits._

  override protected def compute(): DataFrame = dBSource.readFromSource()
    .filter(s"uid == '$user'")
    .filter(s"transactionid == '$transactionId'")
    .select($"uid" as "key", to_json(struct($"*")) as "value")

  override protected def writeData[DataFrameWriter[Row]](): sql.DataFrameWriter[Row] = compute()
    .write
    .format(outputSource.sourceType)
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "C:\\Users\\Alex\\Desktop\\option")
    .option("topic", outputSource.topic)

}
