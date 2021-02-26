package Data

import Prediction.listToTransaction

/**
  * A factory for the data object that are involved in this domain
  */
object DataFactory {

  def createTransaction(l: List[String]): Transaction = l

  def createTransactionTransformed(l: List[String]): TransactionTransformed = l

  def createPrediction(l: List[String]): Prediction = l

}