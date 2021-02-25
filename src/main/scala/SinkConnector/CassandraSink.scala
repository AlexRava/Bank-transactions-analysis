package SinkConnector

import Utility.LoadTableInCassandra
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{ForeachWriter}

abstract class CassandraSink[T] extends ForeachWriter[T]{

  def query(r: T):String //template method

  override def open(partitionId: Long, epochId: Long): Boolean = true

  override def process(r: T)= {
    //println("HERE")
    //import spark.implicits._

    //test di una query vuota

    //il connector deve essere reistanziato tutte le volte |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    //CassandraDriver.connector.withSessionDo(session => {}
    // session.execute("select * from "+CassandraDriver.namespace+"."+CassandraDriver.table+";").forEach(r => println(r.toString))
    //session.execute(""select * from" ${CassandraDriver.namespace}.${CassandraDriver.table}";")
    //)
    //val t = r.asInstanceOf[Transaction]
    //val cols = "DeviceOS, Browser, DeviceType, ScreenResolution , DeviceInfo , uid , TransactionAmt , ProductCD , isFraud , card1 , card2 , card3 , card4 , card5 , card6 , region , country , R_emaildomain , P_emaildomain , TransactionDT , TransactionID ,D1"
    // cassandra insert data OK
    //val driver: DbDriver[CassandraConnector] = CassandraDriver// driver.connector
    //print("sss")
    //val query: String = s""" insert into ${CassandraDriver.keySpace}.${CassandraDriver.table} (${cols}) values('${t.DeviceOS}','${t.Browser}','${t.DeviceType}','${t.ScreenResolution}','${t.DeviceInfo}','${t.uid}',${t.TransactionAmt},'${t.ProductCD}',${t.isFraud},'${t.card1}','${t.card2}','${t.card3}','${t.card4}','${t.card5}','${t.card6}','${t.region}','${t.country}','${t.R_emaildomain}','${t.P_emaildomain}','${t.TransactionDT}','${t.TransactionID}','${t.D1}');"""
    //set the correct spark context
    val connector = CassandraConnector(App.Application.spark.sparkContext.getConf)

    connector.withSessionDo(session => session.execute(query(r)))

  }

  override def close(errorOrNull: Throwable): Unit = {}


}
