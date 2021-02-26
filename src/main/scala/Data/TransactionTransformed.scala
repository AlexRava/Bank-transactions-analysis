package Data

import org.apache.spark.sql.types.StringType
import Transaction.TransactionSchema

/**
  * A transformed transaction type, it represent a specific transaction after the feature engineering phase
  */
case class TransactionTransformed(
                                   DeviceOS: String = "",
                                   Browser: String = "",
                                   DeviceType: String = "",
                                   ScreenResolution: String = "",
                                   DeviceInfo: String = "",
                                   uid: String = "",
                                   TransactionAmt: String = "",
                                   isFraud: String = "",
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
                                   N_Trans_Last_month_TransactionAmt_less_35 : String = "",
                                   N_Trans_Last_month_35_less_TransactionAmt_greater_80: String = "",
                                   N_Trans_Last_month_TransactionAmt_greater_80: String = "",
                                   N_Trans_Last_month :String = "",
                                   IS_TransactionAmt_over_than_perc_of_the_mean_wrt_lastmonth :String = "",
                                   N_Trans_Last_week_TransactionAmt_less :String = "",
                                   N_Trans_Last_week_35_less_TransactionAmt_greater_80 :String = "",
                                   N_Trans_Last_week_TransactionAmt_greater_80 :String = "",
                                   N_Trans_Last_week :String = "",
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

/**
  * Utility for a Transformed transactions data type
  */
object TransactionTransformed{

  /**
    * A schema of a transformed Transaction ( related to json stuff )
    * @return the schema of a transformed transaction
    */
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

  /**
    * An implicit factory for a transformed transaction
    * @param l a list of all the field of a transformed transaction
    * @return a new transformed transaction
    */
  implicit def listToTransactionTransformed(l:List[String]):TransactionTransformed = new TransactionTransformed(
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
    l(17),
    l(18),
    l(19),
    l(20),
    l(21),
    l(22),
    l(23),
    l(24),
    l(25),
    l(27),
    l(28),
    l(29),
    l(30),
    l(31),
    l(32),
    l(33),
    l(34),
    l(35),
    l(35),
    l(37)
  )
}