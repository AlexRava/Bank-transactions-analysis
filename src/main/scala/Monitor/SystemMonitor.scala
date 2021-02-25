package Monitor

import Sources.KafkaSource
import Sources.KafkaSources.{AllTransactionSource, InputSource, PredictionSource, TransactionTransformedSource}


/**
  * Due to a SystemMonitor, it is possible to choose a particular topic source and monitoring it.
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

/**
  * Abstraction for monitoring the system.
  */
object SystemMonitor extends SystemMonitor
