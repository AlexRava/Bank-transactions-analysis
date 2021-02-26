package Data

import org.apache.spark.sql.types.{StringType, StructType}

/**
  * A transaction type, it represent a specific transaction
  */
case class Transaction(DeviceOS: String = "",
                         Browser: String = "",
                         DeviceType: String = "",
                         ScreenResolution: String = "",
                         DeviceInfo: String = "",
                         uid: String = "",
                         TransactionAmt: String = "",
                         isFraud:String = "",
                         card2: String = "",
                         card3: String = "",
                         card4: String = "",
                         card5: String = "",
                         card6: String = "",
                         country: String = "",
                         R_emaildomain: String = "",
                         P_emaildomain: String = "",
                         TransactionDT: String = "",
                         TransactionID: String = "",
                         )

/**
  * Utility for Transactions data type
  */
object Transaction{

  /**
    * A schema of a Transaction ( related to json stuff )
    * @return the schema of a Transaction
    */
  def TransactionSchema = new StructType()
    .add("uid", StringType)
    .add("transactionid", StringType)
    .add("browser", StringType)
    .add("card2", StringType )
    .add("card3", StringType )
    .add("card4", StringType)
    .add("card5", StringType)
    .add("card6", StringType)
    .add("country", StringType )
    .add("deviceinfo", StringType )
    .add("deviceos", StringType)
    .add("devicetype", StringType )
    .add("isfraud", StringType)
    .add("p_emaildomain", StringType)
    .add("r_emaildomain", StringType)
    .add("screenresolution", StringType )
    .add("transactionamt", StringType)
    .add("transactiondt", StringType)

  /**
    * An implicit factory for a transaction
    * @param l a list of all the field of a transaction
    * @return a new transaction
    */
  implicit def listToTransaction(l: List[String]): Transaction = new Transaction(
    l(0),
    l(1),
    l(2),
    l(3),
    l(4),
    l(5),
    l(6),
    l(7),
    l(8),
    l(9),
    l(10),
    l(11),
    l(12),
    l(13),
    l(14),
    l(15),
    l(16),
    l(17)
  )
}
