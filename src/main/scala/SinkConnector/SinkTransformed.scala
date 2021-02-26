package SinkConnector

import Data.TransactionTransformed
import Sources.CassandraSource

class SinkTransformed(val source: CassandraSource) extends CassandraSink[TransactionTransformed] {

  override def query(t: TransactionTransformed): String =
    s""" insert into ${source.namespace}.${source.table} (${source.col}) values('${t.DeviceOS}','${t.Browser}','${t.DeviceType}','${t.ScreenResolution}','${t.DeviceInfo}','${t.uid}','${t.TransactionAmt}','${t.isFraud}','${t.card2}','${t.card3}','${t.card4}','${t.card5}','${t.card6}','${t.country}','${t.R_emaildomain}','${t.P_emaildomain}','${t.TransactionDT}','${t.TransactionID}','${t.N_Trans_Last_month_TransactionAmt_less_35}','${t.N_Trans_Last_month_35_less_TransactionAmt_greater_80}','${t.N_Trans_Last_month_TransactionAmt_greater_80}','${t.N_Trans_Last_month}','${t.IS_TransactionAmt_over_than_perc_of_the_mean_wrt_lastmonth}','${t.N_Trans_Last_week_TransactionAmt_less}','${t.N_Trans_Last_week_35_less_TransactionAmt_greater_80}','${t.N_Trans_Last_week_TransactionAmt_greater_80}','${t.N_Trans_Last_week}','${t.IS_TransactionAmt_over_than_perc_of_the_mean_wrt_lastweek}','${t.IS_DeviceOS_habitual}','${t.IS_Browser_habitual}','${t.IS_DeviceType_habitual}','${t.IS_ScreenResolution_habitual}','${t.IS_DeviceInfo_habitual}','${t.IS_country_habitual}','${t.IS_R_emaildomain_habitual}','${t.IS_P_emaildomain_habitual}','${t.IS_card4_habitual}','${t.IS_card6_habitual}');"""

}

