package Data

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}


case class Transaction(DeviceOS: String = "",
                         Browser: String = "",
                         DeviceType: String = "",
                         ScreenResolution: String = "",
                         DeviceInfo: String = "",
                         uid: String = "",
                         TransactionAmt: String = "",//Double = 0,
                         //ProductCD: String = "",
                         isFraud:String = "",// Int = 0,
                         //card1: String = "",
                         card2: String = "",
                         card3: String = "",
                         card4: String = "",
                         card5: String = "",
                         card6: String = "",
                         //region: String = "",
                         country: String = "",
                         R_emaildomain: String = "",
                         P_emaildomain: String = "",
                         TransactionDT: String = "",//DateTime = new DateTime(),
                         TransactionID: String = "",
                         //D1: String = "",
                        )

object Transaction{

  def TransactionSchema = new StructType()
    .add("uid", StringType)
    .add("transactionid", StringType)
    .add("browser", StringType)
    //.add("card1", StringType )
    .add("card2", StringType )
    .add("card3", StringType )
    .add("card4", StringType)
    .add("card5", StringType)
    .add("card6", StringType)
    .add("country", StringType )
    //.add("d1", StringType)
    .add("deviceinfo", StringType )
    .add("deviceos", StringType)
    .add("devicetype", StringType )
    .add("isfraud", StringType)//IntegerType)
    .add("p_emaildomain", StringType)
    //.add("productcd", StringType)
    .add("r_emaildomain", StringType)
    //.add("region", StringType)
    .add("screenresolution", StringType )
    .add("transactionamt", StringType)//DoubleType)
    .add("transactiondt", StringType) //DateTime = new DateTime(),

  implicit def listToTransaction(l: List[String]): Transaction = new Transaction(
    l(0),
    l(1),
    l(2),
    l(3),
    l(4),
    l(5),
    l(6),//.toDouble,
    l(7),
    l(8),//.toInt,
    l(9),
    l(10),
    l(11),
    l(12),
    l(13),
    l(14),
    l(15),
    l(16),
    l(17)
    /*l(18),
    l(19),
    l(20),
    l(21)*/
  )
}
