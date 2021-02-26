package Data

import org.apache.spark.sql.types.{StringType, StructType}

/**
  * A prediction type, it represent a prediction of a specific transaction
  * @param uid the user that did the transaction
  * @param TransactionID a unic reference to the transactions
  * @param prediction the prediction result: 0: Non Fraud | 1:Fraud
  */
case class Prediction( uid: String = "",
                       TransactionID: String = "",
                       prediction: String = ""
                     )

/**
  * Utility for Prediction data type
  */
object Prediction{

  /**
    * A schema of a Prediction ( related to json stuff )
    * @return the schema of a Prediction
    */
  def PredictionSchema = new StructType()
    .add("uid", StringType)
    .add("transactionid", StringType)
    .add("prediction", StringType)

  /**
    * An implicit factory for a prediction data type
    * @param l a list of all the field of a prediction
    * @return a new prediction
    */
  implicit def listToTransaction(l: List[String]): Prediction  = new Prediction(
    l(0),
    l(1),
    l(2)
  )
}





