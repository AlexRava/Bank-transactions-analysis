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
    override def table: String = "transactions"

    override def col: String = "DeviceOS, Browser, DeviceType, ScreenResolution , DeviceInfo , uid , TransactionAmt , ProductCD , isFraud , card1 , card2 , card3 , card4 , card5 , card6 , region , country , R_emaildomain , P_emaildomain , TransactionDT , TransactionID ,D1"

  }

  /**
    * Thank's to this Cassandra source abstractions, it is possible to
    * reference a table where are stored all the results of the AI model.
    */
  object DbResult extends BankCassandraSource{
    override def table: String = "result"

    override def col: String = "uid, TransactionID, prediction"


  }

  /**
    * Thank's to this Cassandra source abstractions, it is possible to
    * reference a table where are stored all the transformed transactions.
    */
  object DbTransformed extends SimulationCassandraSource{
    override def table: String = "transformed"

    override def col: String = "DeviceOS, Browser, DeviceType, ScreenResolution , DeviceInfo , uid , TransactionAmt , ProductCD , isFraud , card1 , card2 , card3 , card4 , card5 , card6 , region , country , R_emaildomain , P_emaildomain , TransactionDT , TransactionID , D1 , N_Trans_Last_month_TransactionAmt_less_35 , N_Trans_Last_month_35_less_TransactionAmt_greater_80 , N_Trans_Last_month_TransactionAmt_greater_80 , N_Trans_Last_month , IS_TransactionAmt_over_than_perc_of_the_mean_wrt_lastmonth , N_Trans_Last_week_TransactionAmt_less , N_Trans_Last_week_35_less_TransactionAmt_greater_80 , N_Trans_Last_week_TransactionAmt_greater_80 , N_Trans_Last_week , IS_TransactionAmt_over_than_perc_of_the_mean_wrt_lastweek , IS_DeviceOS_habitual , IS_Browser_habitual , IS_DeviceType_habitual , IS_ScreenResolution_habitual , IS_DeviceInfo_habitual , IS_country_habitual , IS_R_emaildomain_habitual , IS_P_emaildomain_habitual , IS_card4_habitual , IS_card6_habitual"

  }

  /**
    * Thank's to this Cassandra source abstractions, it is possible to
    * reference a table where are stored all the predictions done by the AI model.
    */
  object DbPrediction extends SimulationCassandraSource{
    override def table: String = "prediction"

    override def col: String = "uid, TransactionID, prediction"
  }

}
