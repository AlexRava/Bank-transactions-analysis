package Sources

/**
  * Reference for Cassandra source abstractions.
  */
object CassandraSources {

  /**
    * Thank's to this Cassandra source abstractions, it is possible to
    * reference a table where are stored all the historical data of all the users.
    */
  object DbHistoricalData extends BankCassandraSource{
    override def table: String = "transactions1"
  }

  /**
    * Thank's to this Cassandra source abstractions, it is possible to
    * reference a table where are stored all the results of the AI model.
    */
  object DbResult extends BankCassandraSource{
    override def table: String = "result"
  }

  /**
    * Thank's to this Cassandra source abstractions, it is possible to
    * reference a table where are stored all the transformed transactions.
    */
  object DbTransformedSource extends SimulationCassandraSource{
    override def table: String = "transformed"
  }

  /**
    * Thank's to this Cassandra source abstractions, it is possible to
    * reference a table where are stored all the predictions done by the AI model.
    */
  object DbPrediction extends SimulationCassandraSource{
    override def table: String = "prediction"
  }

}
