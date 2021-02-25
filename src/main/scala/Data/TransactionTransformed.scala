package Data

import org.apache.spark.sql.types.StringType
import Transaction.TransactionSchema

case class TransactionTransformed(
                                   DeviceOS: String = "",
                                   Browser: String = "",
                                   DeviceType: String = "",
                                   ScreenResolution: String = "",
                                   DeviceInfo: String = "",
                                   uid: String = "",
                                   TransactionAmt: String = "",//Double = 0,
                                   //ProductCD: String = "",
                                   isFraud: String = "",//Int = 0,
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
                                   TransactionDT: String = "",
                                   TransactionID: String = "",
                                   //D1: String = "",
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

object TransactionTransformed{
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
    l(37)
    /*l(38),
    l(39),
    l(40),
    l(41),*/
  )
}