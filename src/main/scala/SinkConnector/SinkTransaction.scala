package SinkConnector

import Data.{Prediction, Transaction}
import Sources.CassandraSource
import org.apache.spark.sql.Row

class SinkTransaction (val source: CassandraSource) extends CassandraSink {

  def query(r: Row): String = {
    val t = r.asInstanceOf[Transaction]
    s""" insert into ${source.namespace}.${source.table} (${source.col}) values('${t.DeviceOS}','${t.Browser}','${t.DeviceType}','${t.ScreenResolution}','${t.DeviceInfo}','${t.uid}',${t.TransactionAmt},'${t.ProductCD}',${t.isFraud},'${t.card1}','${t.card2}','${t.card3}','${t.card4}','${t.card5}','${t.card6}','${t.region}','${t.country}','${t.R_emaildomain}','${t.P_emaildomain}','${t.TransactionDT}','${t.TransactionID}','${t.D1}');"""
  }

}