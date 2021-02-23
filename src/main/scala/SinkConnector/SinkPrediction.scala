package SinkConnector
import Data.Prediction
import Sources.CassandraSource
import org.apache.spark.sql.Row

class SinkPrediction(val source: CassandraSource) extends CassandraSink {

  def query(r: Row): String = {
    val p = r.asInstanceOf[Prediction]
    s""" insert into ${source.namespace}.${source.table} (${source.col}) values('${p.uid}','${p.TransactionID}','${p.prediction}'"""
  }
}
