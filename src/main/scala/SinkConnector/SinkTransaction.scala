package SinkConnector

import Data.Transaction
import Sources.CassandraSource


class SinkTransaction (val source: CassandraSource) extends CassandraSink[Transaction] {

  override def query(t: Transaction): String =
    s""" insert into ${source.namespace}.${source.table} (${source.col}) values('${t.DeviceOS}','${t.Browser}','${t.DeviceType}','${t.ScreenResolution}','${t.DeviceInfo}','${t.uid}','${t.TransactionAmt}','${t.isFraud}','${t.card2}','${t.card3}','${t.card4}','${t.card5}','${t.card6}','${t.country}','${t.R_emaildomain}','${t.P_emaildomain}','${t.TransactionDT}','${t.TransactionID}');"""


}