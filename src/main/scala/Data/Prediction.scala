package Data

import org.apache.spark.sql.types.{StringType, StructType}

case class Prediction( uid: String = "",
                       TransactionID: String = "",
                       prediction: String = ""
                     )

object Prediction{

  def PredictionSchema = new StructType()
    .add("uid", StringType)
    .add("transactionid", StringType)
    .add("prediction", StringType)

  implicit def listToTransaction(l: List[String]): Prediction  = new Prediction(
    l(0),
    l(1),
    l(2)
  )
}





