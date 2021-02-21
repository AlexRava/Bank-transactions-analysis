package Sources

/**
  * A very simple source of data, it could be specialized by a more specific source type.
  */
trait Source{
  def sourceType = "simple-source"
}

/**
  * A Kafka source of data, this abstraction represent a Kafka Topic.
  */
trait KafkaSource extends Source{
  override def sourceType: String = "kafka"
  def topic:String
}

/**
  * A Cassandra source of data, this abstraction represent a Cassandra Database.
  */
trait CassandraSource extends Source{
  override def sourceType: String = "DB-CASSANDRA"
  def table:String
  def namespace:String
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


