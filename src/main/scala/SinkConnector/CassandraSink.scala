package SinkConnector

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.ForeachWriter

/*
provare a sostiruti T con Transaction ( anche nel metodo process ) probabilmente non funziona
mettere ForeachWriter[Row] potrebbe essere che se poi voglio usare Transaction, Transaction deve estendere Row..
 */
class SinkConnctor[Transaction] extends ForeachWriter[Transaction]{

  override def open(partitionId: Long, epochId: Long): Boolean = true

  override def process(value: Transaction): Unit = {


    val sparkConf = App.spark.sparkContext.getConf

    //import spark.implicits._

    val connector = CassandraConnector(sparkConf)
    //test di una query vuota

    //il connector deve essere reistanziato tutte le volte |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    connector.withSessionDo(session => {}
    // session.execute("select * from "+CassandraDriver.namespace+"."+CassandraDriver.table+";").forEach(r => println(r.toString))
    //session.execute(""select * from" ${CassandraDriver.namespace}.${CassandraDriver.table}";")
    //)

    // cassandra insert data OK

        connector.withSessionDo(session =>
          session.execute(s"""
           insert into ${CassandraDriver.namespace}.${CassandraDriver.table} (uid)
           values('${value}')""")
        )

    /*var cassandraDriver = CassandraDriver
  if (cassandraDriver == null) {
  cassandraDriver = new CassandraDriver();
  }*/

  }

  override def close(errorOrNull: Throwable): Unit = {}

}
