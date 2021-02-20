package SinkConnector

import Data.DataObject.Transaction
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{ForeachWriter, Row}

/*
provare a sostiruti T con Transaction ( anche nel metodo process ) probabilmente non funziona
mettere ForeachWriter[Row] potrebbe essere che se poi voglio usare Transaction, Transaction deve estendere Row..
 */
class CassandraSink extends ForeachWriter[Row]{

  override def open(partitionId: Long, epochId: Long): Boolean = true

  override def process(r: Row/*Transaction*/): Unit = {
    //import spark.implicits._

    //test di una query vuota

    //il connector deve essere reistanziato tutte le volte |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    //CassandraDriver.connector.withSessionDo(session => {}
    // session.execute("select * from "+CassandraDriver.namespace+"."+CassandraDriver.table+";").forEach(r => println(r.toString))
    //session.execute(""select * from" ${CassandraDriver.namespace}.${CassandraDriver.table}";")
    //)
    val t = r.asInstanceOf[Transaction]
    val cols = "DeviceOS, Browser, DeviceType, ScreenResolution , DeviceInfo , uid , TransactionAmt , ProductCD , isFraud , card1 , card2 , card3 , card4 , card5 , card6 , region , country , R_emaildomain , P_emaildomain , TransactionDT , TransactionID ,D1"
    // cassandra insert data OK
    val driver: DbDriver[CassandraConnector] = CassandraDriver

    val query: String = s""" insert into ${CassandraDriver.keySpace}.${CassandraDriver.table} (${cols}) values('${t.DeviceOS}','${t.Browser}','${t.DeviceType}','${t.ScreenResolution}','${t.DeviceInfo}','${t.uid}',${t.TransactionAmt},'${t.ProductCD}',${t.isFraud},'${t.card1}','${t.card2}','${t.card3}','${t.card4}','${t.card5}','${t.card6}','${t.region}','${t.country}','${t.R_emaildomain}','${t.P_emaildomain}','${t.TransactionDT}','${t.TransactionID}','${t.D1}');"""

    driver.connector.withSessionDo(session => session.execute(query))

  }

  override def close(errorOrNull: Throwable): Unit = {}

  /*
  object SinkCassandraHelper{

    getCreateTableSchema() = {}
  }*/

}
