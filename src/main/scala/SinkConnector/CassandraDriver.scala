package SinkConnector

import App._
import Utility.LoadTableInCassandra
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder._;


trait DbDriver[T] {
/*
  def newKeyspace(keyspaceName:String, replicationFactor: Int)
  def removeKeySpace(keyspaceName:String)

  def newTable(tableName:String)
  def removeTable(tableName:String)*/

  def connector():T

  /*def keySpace:String
  def table:String*/

}

object CassandraDriver extends DbDriver[CassandraConnector]{

  private var conn: Option[CassandraConnector] = Option.empty

  /*override def newKeyspace(keySpaceName: String, replicationFactor: Int) = createKeyspace(keySpaceName).ifNotExists().withSimpleStrategy(replicationFactor)

  override def removeKeySpace(keySpaceName: String) = dropKeyspace(keySpaceName).ifExists()

  override def newTable(newTableQuery: String) = CqlSession.builder().build().execute(newTableQuery)

  override def removeTable(tableName: String) = dropTable(tableName).ifExists()*/

  //deve essere sempre reinstanziato o può essere riuato ?  se puo essere riusato uso il metodo qui sotto, altrimenti uso val connector = CassandraConnector(App.spark.sparkContext.getConf)
  //  spostare questa implentazione in driver ?

  //BUONO MA NON FUNZIONAAAAAA, PROBLEMA DA DOVE LO CHIAMO App.spark.ecc se sono i load table devo fare riferimento a quel spark..
  /*override def connector():CassandraConnector = conn match {
    case None => {
      conn = Option(CassandraConnector(App.spark.sparkContext.getConf))
      conn.get
    }
    case _ => conn.get
  }*/
  //valido solo se faccio da load table in cassandra
  val connector = CassandraConnector(LoadTableInCassandra.spark.sparkContext.getConf)

  val  keySpace = "bank"
  val table = "transactions1"


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
