package SinkConnector
import Data.Prediction
import Sources.CassandraSource

class SinkPrediction(val source: CassandraSource) extends CassandraSink[Prediction] {

  override def query(p: Prediction): String =
    s""" insert into ${source.namespace}.${source.table} (${source.col}) values('${p.uid}','${p.TransactionID}','${p.prediction}');"""

}
