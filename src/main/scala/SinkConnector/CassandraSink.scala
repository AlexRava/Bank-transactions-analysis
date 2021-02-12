package SinkConnector

import Data.DataObject.Transaction
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.ForeachWriter

/*
provare a sostiruti T con Transaction ( anche nel metodo process ) probabilmente non funziona
mettere ForeachWriter[Row] potrebbe essere che se poi voglio usare Transaction, Transaction deve estendere Row..
 */
class SinkConnctor extends ForeachWriter[Transaction]{

  override def open(partitionId: Long, epochId: Long): Boolean = true

  override def process(value: Transaction): Unit = {
    //import spark.implicits._

    //test di una query vuota

    //il connector deve essere reistanziato tutte le volte |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    //CassandraDriver.connector.withSessionDo(session => {}
    // session.execute("select * from "+CassandraDriver.namespace+"."+CassandraDriver.table+";").forEach(r => println(r.toString))
    //session.execute(""select * from" ${CassandraDriver.namespace}.${CassandraDriver.table}";")
    //)

    // cassandra insert data OK
    val driver: Driver[CassandraConnector] = CassandraDriver

    driver.connector.withSessionDo(session => session.execute
      (s""" insert into ${CassandraDriver.keySpace}.${CassandraDriver.table} (uid)
           values('${value}')""")
      )


  }

  override def close(errorOrNull: Throwable): Unit = {}

}
