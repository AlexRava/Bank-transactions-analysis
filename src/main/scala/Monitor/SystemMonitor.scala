package Monitor

import Sources.KafkaSource
import Sources.KafkaSources.{AllTransactionSource, InputSource, PredictionSource, TransactionTransformedSource}


/**
  * Due to a SystemMonitor, it is possible to choose a particular topic and monitor it.
  */
trait SystemMonitor {

  def monitorTopic(s: KafkaSource) = MonitorTopic.fromTopicSource(s).monitor()

  def monitorInput() =  monitorTopic(InputSource)

  def monitorAllTransactions() =  monitorTopic(AllTransactionSource)

  def monitorTransactionTransformed() = monitorTopic(TransactionTransformedSource)

  def monitorPredictions() =  monitorTopic(PredictionSource)

  def monitorAll() =  {
    monitorInput()
    monitorAllTransactions()
    monitorTransactionTransformed()
    monitorPredictions()
  }
}
/*
object StartMonitoring {

  def monitorInput() ={

  }

  def monitorAllTransactions() = {

  }

  def monitorTransactionTransformed() = {

  }

  def monitorPredictions() = {

  }

  def monitorAll() ={

  }
  println(InputSource.readFromSource())
  //MonitorTopic.fromTopicSource(InputSource).printData()


  var allTransactionsTopic = MonitorTopic.fromTopicSource(AllTransactionSource)
  var transactionTransformedTopic = MonitorTopic.fromTopicSource(TransactionTransformedSource)
  var predictionTopic = MonitorTopic.fromTopicSource(PredictionSource)
}
*/