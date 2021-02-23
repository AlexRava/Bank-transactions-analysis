package Data

object DataFactory {

  def createTransaction(l: List[String]): Transaction = l

  def createTransactionTransformed(l: List[String]): TransactionTransformed = l

  def createPrediction(l: List[String]): Prediction = l

}