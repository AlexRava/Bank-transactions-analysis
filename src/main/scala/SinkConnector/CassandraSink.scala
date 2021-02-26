package SinkConnector

import Utility.LoadTableInCassandra
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.ForeachWriter

abstract class CassandraSink[T] extends ForeachWriter[T]{

  def query(r: T):String

  override def open(partitionId: Long, epochId: Long): Boolean = true

  override def process(r: T)= {
    val connector = CassandraConnector(App.Application./*LoadTableInCassandra.*/spark.sparkContext.getConf)
    connector.withSessionDo(session => session.execute(query(r)))
  }

  override def close(errorOrNull: Throwable): Unit = {}


}
