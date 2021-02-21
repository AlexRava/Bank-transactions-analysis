package Sources

object CassandraSources {

  /**
    * Thank's to this cassandra source abstractions, it is possible to
    * reference a table were are stored all the historical data of all the users
    */
  object DbHistoricalData extends BankCassandraSource{
    override def table: String = "transactions1"
  }

  /**
    * Thank's to this
    *
    */
  object DbTransactionSource extends Bank



  object DbTransformedSource extends BankCassandraSource{
    override def table: String = ""
  }

  object Db

}

}
