package Data


import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.types.{IntegerType, _}
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
    def createTransaction(l: List[String]): Transaction = l
    def createTransactionTransformed(l: List[String]): TransactionTransformed = l
  }

  object Transaction{

    def TransactionSchema = new StructType()
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

    def TransactionTransformedSchema = TransactionSchema
      .add("n_trans_last_month_transactionamt_less_35",StringType)
      .add("n_trans_last_month_35_less_transactionamt_greater_80" ,StringType)
      .add("n_trans_last_month_transactionamt_greater_80" ,StringType)
      .add("n_trans_last_month" ,StringType)
      .add("is_transactionamt_over_than_perc_of_the_mean_wrt_lastmonth" ,StringType)
      .add("n_trans_last_week_transactionamt_less" ,StringType)
      .add("n_trans_last_week_35_less_transactionamt_greater_80" ,StringType)
      .add("n_trans_last_week_transactionamt_greater_80" ,StringType)
      .add("n_trans_last_week",StringType)
      .add("is_transactionamt_over_than_perc_of_the_mean_wrt_lastweek" ,StringType)
      .add("is_deviceos_habitual" ,StringType)
      .add("is_browser_habitual" ,StringType)
      .add("is_devicetype_habitual" ,StringType)
      .add("is_screenresolution_habitual" ,StringType)
      .add("is_deviceinfo_habitual",StringType)
      .add("is_country_habitual" ,StringType)
      .add("is_r_emaildomain_habitual" ,StringType)
      .add("is_p_emaildomain_habitual" ,StringType)
      .add("is_card4_habitual" ,StringType)
      .add("is_card6_habitual" ,StringType)


    /*{
      val schema = new StructType()
      schema.add("DeviceOS", IntegerType)
      schema
    }*/
    //def apply(list: List[String]): Transaction = list
  }

  //l'laternativa sarebbe avere un TransAction factory che quando crea la transaction il metodo lo fa con il corpo del metodo implicito

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
    l(17),
    l(18),
    l(19),
    l(20),
    l(21)
  )

  implicit def listToTransactionTransformed(l:List[String]):TransactionTransformed = new TransactionTransformed(
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
    l(17),
    l(18),
    l(19),
    l(20),
    l(21),
    l(22),//.toDouble.toInt,
    l(23),//.toDouble.toInt,
    l(24),//.toDouble.toInt,
    l(25),//.toDouble.toInt,
    l(27),
    l(28),//.toDouble.toInt,
    l(29),//.toDouble.toInt,
    l(30),//.toDouble.toInt,
    l(31),//.toDouble.toInt,
    l(32),
    l(33),
    l(34),
    l(35),
    l(35),
    l(37),
    l(38),
    l(39),
    l(40),
    l(41),
  )

  case class Transaction(DeviceOS: String = "",
                         Browser: String = "",
                         DeviceType: String = "",
                         ScreenResolution: String = "",
                         DeviceInfo: String = "",
                         uid: String = "",
                         TransactionAmt: String = "",//Double = 0,
                         ProductCD: String = "",
                         isFraud:String = "",// Int = 0,
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
  case class TransactionTransformed(
                                     DeviceOS: String = "",
                                     Browser: String = "",
                                     DeviceType: String = "",
                                     ScreenResolution: String = "",
                                     DeviceInfo: String = "",
                                     uid: String = "",
                                     TransactionAmt: String = "",//Double = 0,
                                     ProductCD: String = "",
                                     isFraud: String = "",//Int = 0,
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
                                     TransactionDT: String = "",
                                     TransactionID: String = "",
                                     D1: String = "",
                                     N_Trans_Last_month_TransactionAmt_less_35 : String = "",//Int = 0,
                                     N_Trans_Last_month_35_less_TransactionAmt_greater_80: String = "",//Int = 0,
                                     N_Trans_Last_month_TransactionAmt_greater_80: String = "",//Int = 0,
                                     N_Trans_Last_month :String = "",//Int = 0,
                                     IS_TransactionAmt_over_than_perc_of_the_mean_wrt_lastmonth :String = "",
                                     N_Trans_Last_week_TransactionAmt_less :String = "",//Int = 0,
                                     N_Trans_Last_week_35_less_TransactionAmt_greater_80 :String = "",//Int = 0,
                                     N_Trans_Last_week_TransactionAmt_greater_80 :String = "",//Int = 0,
                                     N_Trans_Last_week :String = "",//Int = 0,
                                     IS_TransactionAmt_over_than_perc_of_the_mean_wrt_lastweek :String = "",
                                     IS_DeviceOS_habitual :String = "",
                                     IS_Browser_habitual :String = "",
                                     IS_DeviceType_habitual :String = "",
                                     IS_ScreenResolution_habitual :String = "",
                                     IS_DeviceInfo_habitual :String = "",
                                     IS_country_habitual :String = "",
                                     IS_R_emaildomain_habitual :String = "",
                                     IS_P_emaildomain_habitual :String = "",
                                     IS_card4_habitual :String = "",
                                     IS_card6_habitual :String = "",
                                   )
}
