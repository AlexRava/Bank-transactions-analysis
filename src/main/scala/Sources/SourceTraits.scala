package Sources


trait Source{
  def sourceType:String
}

trait KafkaSource extends Source{
  override def sourceType: String = "TOPIC-KAFKA"
  def topic:String
}

trait CassandraSource extends Source{
  override def sourceType: String = "DB-CASSANDRA"
  def table:String
  def namespace:String
}

trait BankCassandraSource extends CassandraSource{
  override def namespace: String = "bank"
}

trait SimularionCassabdraSource extends CassandraSource{
  override def namespace: String = "simulation"
}


