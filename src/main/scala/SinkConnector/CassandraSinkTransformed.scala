package SinkConnector

import Data.DataObject.{Transaction, TransactionTransformed}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.ForeachWriter

class CassandraSinkTransformed extends ForeachWriter[TransactionTransformed]{
  //var cont = 0
  override def open(partitionId: Long, epochId: Long): Boolean = true

  override def process(t: TransactionTransformed): Unit = {
    val cols = "DeviceOS, Browser, DeviceType, ScreenResolution , DeviceInfo , uid , TransactionAmt , ProductCD , isFraud , card1 , card2 , card3 , card4 , card5 , card6 , region , country , R_emaildomain , P_emaildomain , TransactionDT , TransactionID ,D1, N_Trans_Last_month_TransactionAmt_less_35, N_Trans_Last_month_35_less_TransactionAmt_greater_80, N_Trans_Last_month_TransactionAmt_greater_80, N_Trans_Last_month, IS_TransactionAmt_over_than_perc_of_the_mean_wrt_lastmonth, N_Trans_Last_week_TransactionAmt_less, N_Trans_Last_week_35_less_TransactionAmt_greater_80, N_Trans_Last_week_TransactionAmt_greater_80, N_Trans_Last_week, IS_TransactionAmt_over_than_perc_of_the_mean_wrt_lastweek, IS_DeviceOS_habitual, IS_Browser_habitual, IS_DeviceType_habitual, IS_ScreenResolution_habitual, IS_DeviceInfo_habitual, IS_country_habitual, IS_R_emaildomain_habitual, IS_P_emaildomain_habitual, IS_card4_habitual, IS_card6_habitual"
    val driver: DbDriver[CassandraConnector] = CassandraDriver

    val query: String =
      s""" insert into ${CassandraDriver.keySpace}.transformed_transactions (${cols}) values('${t.DeviceOS}','${t.Browser}','${t.DeviceType}','${t.ScreenResolution}','${t.DeviceInfo}','${t.uid}',${t.TransactionAmt},'${t.ProductCD}',${t.isFraud},'${t.card1}','${t.card2}','${t.card3}','${t.card4}','${t.card5}','${t.card6}','${t.region}','${t.country}','${t.R_emaildomain}','${t.P_emaildomain}','${t.TransactionDT}','${t.TransactionID}','${t.D1}',${t.N_Trans_Last_month_TransactionAmt_less_35},${t.N_Trans_Last_month_35_less_TransactionAmt_greater_80},${t.N_Trans_Last_month_TransactionAmt_greater_80},${t.N_Trans_Last_month},'${t.IS_TransactionAmt_over_than_perc_of_the_mean_wrt_lastmonth}',${t.N_Trans_Last_week_TransactionAmt_less},${t.N_Trans_Last_week_35_less_TransactionAmt_greater_80},${t.N_Trans_Last_week_TransactionAmt_greater_80},${t.N_Trans_Last_week},'${t.IS_TransactionAmt_over_than_perc_of_the_mean_wrt_lastweek}','${t.IS_DeviceOS_habitual}','${t.IS_Browser_habitual}','${t.IS_DeviceType_habitual}','${t.IS_ScreenResolution_habitual}','${t.IS_DeviceInfo_habitual}','${t.IS_country_habitual}','${t.IS_R_emaildomain_habitual}','${t.IS_P_emaildomain_habitual}','${t.IS_card4_habitual}','${t.IS_card6_habitual}');"""
    driver.connector.withSessionDo(session => session.execute(query))
    //cont = cont+1
    //println(cont)
  }


  override def close(errorOrNull: Throwable): Unit = {}
}



