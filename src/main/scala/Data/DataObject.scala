package Data


import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.types._
//import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType, StringType}
import org.joda.time.DateTime

object DataObject {

  /*trait Creatable[T <: Creatable[T]] {
    val cs = this.getClass.getConstructors

    def apply(params: Seq[Any]) = {
      cs(0).newInstance(params map {
        _.asInstanceOf[AnyRef]
      }: _*).asInstanceOf[T]
    }
  }*/

  //type TransactionSerialized = (Int, Int, Int, Int, Int, Int, Double, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, String, Int, Int)

  object TransactionFactory {
    //def createTransaction(t:TransactionSerialized): Transaction = Transaction(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13, t._14, t._15, t._16, t._17, t._18, t._19, t._20, t._21, t._22)
    /*def createTransaction(l: List[String]): Transaction =
      Transaction(
        l(0).toInt,
        l(1).toInt,
        l(2).toInt,
        l(3).toInt,
        l(4).toInt,
        l(5).toInt,
        l(6).toDouble,
        l(7).toInt,
        l(8).toInt,
        l(9).toInt,
        l(10).toInt,
        l(11).toInt,
        l(12).toInt,
        l(13).toInt,
        l(14).toInt,
        l(15).toInt,
        l(16).toInt,
        l(17).toInt,
        l(18).toInt,
        l(19).asInstanceOf[String],
        l(20).toInt,
        l(21).toDouble.toInt
      )*/
  }

  object Transaction{

    def schema = new StructType()
      .add("uid", StringType)
      .add("transactionid", StringType)
      .add("browser", StringType)
      .add("card1", StringType )
      .add("card2", StringType )
      .add("card3", StringType )
      .add("card4", StringType)
      .add("card5", StringType)
      .add("card6", StringType)
      .add("country", StringType )
      .add("d1", StringType)
      .add("deviceinfo", StringType )
      .add("deviceos", StringType)
      .add("devicetype", StringType )
      .add("isfraud", StringType)//IntegerType)
      .add("p_emaildomain", StringType)
      .add("productcd", StringType)
      .add("r_emaildomain", StringType)
      .add("region", StringType)
      .add("screenresolution", StringType )
      .add("transactionamt", StringType)//DoubleType)
      .add("transactiondt", StringType) //DateTime = new DateTime(),

    /*{
      val schema = new StructType()
      schema.add("DeviceOS", IntegerType)
      schema
    }*/

    def apply(list: List[String]): Transaction = list
  }

  //l'laternativa sarebbe avere un Transiction factory che quando crea la transaction il metodo lo fa con il corpo del metodo implicito

  implicit def listToTransaction(l: List[String]): Transaction = new Transaction(
    l(0),
    l(1),
    l(2),
    l(3),
    l(4),
    l(5),
    l(6).toDouble,
    l(7),
    l(8).toInt,
    l(9),
    l(10),
    l(11),
    l(12),
    l(13),
    l(14),
    l(15),
    l(16),
    l(17),
    l(18),
    l(19),
    l(20),
    l(21)
  )

  case class Transaction(DeviceOS: String = "",
                         Browser: String = "",
                         DeviceType: String = "",
                         ScreenResolution: String = "",
                         DeviceInfo: String = "",
                         uid: String = "",
                         TransactionAmt: Double = 0,
                         ProductCD: String = "",
                         isFraud: Int = 0,
                         card1: String = "",
                         card2: String = "",
                         card3: String = "",
                         card4: String = "",
                         card5: String = "",
                         card6: String = "",
                         region: String = "",
                         country: String = "",
                         R_emaildomain: String = "",
                         P_emaildomain: String = "",
                         TransactionDT: String = "",//DateTime = new DateTime(),
                         TransactionID: String = "",
                         D1: String = "",
                        ) {
    //implicit def tupleToTransactions(t: TransactionSerialized): TransactionSerialized = (t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13, t._14, t._15, t._16, t._17, t._18, t._19, t._20, t._21, t._22)
  }
  case class TransactionTransformed() extends Transaction


}
