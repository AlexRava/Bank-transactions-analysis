package Sources


object KafkaSources {

  object InputSource extends KafkaSource {
    override def topic: String = "INPUT-TOPIC"
  }

  object AllTransactionSource extends KafkaSource {
    override def topic: String = "ALL-TRANSACTIONS-TOPIC"
  }

  object TransactionTransformedSource extends KafkaSource {
    override def topic: String = "TRANSACTIONS-TRANSFORMED-TOPIC"
  }

  object ResultSource extends KafkaSource {
    override def topic: String = "RESULTS-TOPIC"
  }

}
