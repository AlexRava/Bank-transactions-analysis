package SinkConnector

import App._

import com.datastax.spark.connector.cql.CassandraConnector

trait Driver[T] {

  def connector():T

  def keySpace:String
  def table:String

}

object CassandraDriver extends Driver[CassandraConnector]{

  private var conn: Option[CassandraConnector] = Option.empty

  //deve essere sempre reinstanziato o può essere riuato ?  se puo essere riusato uso il metodo qui sotto, altrimenti uso val connector = CassandraConnector(App.spark.sparkContext.getConf)
  //  spostare questa implentazione in driver ?
  override def connector():CassandraConnector = conn match {
    case None => {
      conn = Option(CassandraConnector(App.spark.sparkContext.getConf))
      conn.get
    }
    case _ => conn.get
  }
  //val connector = CassandraConnector(App.spark.sparkContext.getConf)

  val  keySpace = "tutorial"
  val table = "table3"


  //val StreamProviderTableSink = "radioothersink"
  //val kafkaMetadata = "kafkametadata"
  /*def getTestInfo() = {
    val rdd = spark.sparkContext.cassandraTable(namespace, kafkaMetadata)

    if( !rdd.isEmpty ) {
      log.warn(rdd.count)
      log.warn(rdd.first)
    } else {
      log.warn(s"$namespace, $kafkaMetadata is empty in cassandra")
    }*/


  // Define Cassandra's table which will be used as a sink
  /* For this app I used the following table:
       CREATE TABLE fx.spark_struct_stream_sink (
       fx_marker text,
       timestamp_ms timestamp,
       timestamp_dt date,
       primary key (fx_marker));
  */
  /*val namespace = "fx"
  val foreachTableSink = "spark_struct_stream_sink"*/

}
