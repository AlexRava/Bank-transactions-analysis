package Transformer

import App.App.spark
import Data.DataObject.Transaction
import Streams.StreamingFlow

import scala.collection.mutable

trait Transformer{
  def addSource(dataSource: StreamingFlow)
  def compute()


}

class DataTransformer()/*var dataSource: StreamingFlow)*/ extends Transformer {

  import spark.implicits._


  var dataSources = mutable.Set[StreamingFlow]()

  //override def setSource(dataSource: StreamingFlow) = this.dataSource = dataSource
  //override def setSource(dataSource: StreamingFlow*): Unit = this.dataSource. = dataSource

  override def addSource(dataSource: StreamingFlow) = this.dataSources.add(dataSource)

  //deve essere incapusulato da qualche parte come mia strategia "non va bene prendere la testa, devo prendere l'input stream
  private def mergeStream() = dataSources.head.readStream()

  override def compute( ) = {
    mergeStream()
      //.starrt


  }
}
