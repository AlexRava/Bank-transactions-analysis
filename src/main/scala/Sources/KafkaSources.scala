package Sources

/**
* Reference for Kafka-topic source abstractions.
*/
object KafkaSources {

  /**
    * Thank's to this Kafka source abstractions, it is possible to
    * reference a topic where there are all the input transactions.
    */
  object InputSource extends KafkaSource {
    override def topic: String = "INPUT-TOPIC"
  }

  /**
    * Thank's to this Kafka source abstractions, it is possible to
    * reference a topic where there are for each user, all of his transactions.
    */
  object AllTransactionSource extends KafkaSource {
    override def topic: String = "ALL-TRANSACTIONS-TOPIC"
  }

  /**
    * Thank's to this Kafka source abstractions, it is possible to
    *  reference a topic where there are the transformed transactions.
    */
  object TransactionTransformedSource extends KafkaSource {
    override def topic: String = "TRANSACTIONS-TRANSFORMED-TOPIC"
  }

  /**
    * Thank's to this Kafka source abstractions, it is possible to
    *  reference a topic where there are all the predictions.
    */
  object PredictionSource extends KafkaSource {
    override def topic: String = "PREDICTION-TOPIC"
  }

}
