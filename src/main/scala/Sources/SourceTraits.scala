package Sources
import org.apache.spark.sql.cassandra._
import App.Application.spark
import org.apache.spark.sql.DataFrame

/**
  * A very simple source of data, it could be specialized by a more specific source type.
  * It's possible to read from a source of data
  */
trait Source{
  def sourceType = "simple-source"
  def name:String
  def readFromSource(): DataFrame
}

/**
  * A Kafka source of data, this abstraction represent a Kafka Topic.
  */
trait KafkaSource extends Source{
  override def sourceType: String = "kafka"
  override def name = topic

  def topic:String

  override def readFromSource(): DataFrame = spark
      .readStream
      .format(sourceType)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .load()
}

/**
  * A Cassandra source of data, this abstraction represent a Cassandra Database.
  */
trait CassandraSource extends Source{
  override def sourceType: String = "DB-CASSANDRA"
  override def name = namespace+"."+table

  def table:String
  def namespace:String

  override def readFromSource(): DataFrame = spark
    .read
    .cassandraFormat(table , namespace)
    .load()
}

/**
  * An abstraction of a CassandraSource that represent the correct working space inside Cassandra.
  */
trait BankCassandraSource extends CassandraSource{
  override def namespace: String = "bank"
}

/**
  * An abstraction of a CassandraSource that represent a working space inside Cassandra for the simulation purpose.
 */
trait SimulationCassandraSource extends CassandraSource{
  override def namespace: String = "simulation"
}


