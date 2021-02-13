package Data

object DataObject {

  /*trait Creatable[T <: Creatable[T]] {
    val cs = this.getClass.getConstructors

    def apply(params: Seq[Any]) = {
      cs(0).newInstance(params map {
        _.asInstanceOf[AnyRef]
      }: _*).asInstanceOf[T]
    }
  }*/

  //type TransactionSerialized = (Int, Int, Int, Int, Int, Int, Double, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, String, Int, Int)

  object TransactionFactory {
    //def createTransaction(t:TransactionSerialized): Transaction = Transaction(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13, t._14, t._15, t._16, t._17, t._18, t._19, t._20, t._21, t._22)
    /*def createTransaction(l: List[String]): Transaction =
      Transaction(
        l(0).toInt,
        l(1).toInt,
        l(2).toInt,
        l(3).toInt,
        l(4).toInt,
        l(5).toInt,
        l(6).toDouble,
        l(7).toInt,
        l(8).toInt,
        l(9).toInt,
        l(10).toInt,
        l(11).toInt,
        l(12).toInt,
        l(13).toInt,
        l(14).toInt,
        l(15).toInt,
        l(16).toInt,
        l(17).toInt,
        l(18).toInt,
        l(19).asInstanceOf[String],
        l(20).toInt,
        l(21).toDouble.toInt
      )*/
  }

  object Transaction{
    def apply(list: List[String]): Transaction = list
  }

  //l'laternativa sarebbe avere un Transiction factory che quando crea la transaction il metodo lo fa con il corpo del metodo implicito

  implicit def listToTransaction(l: List[String]): Transaction = new Transaction(
    l(0).toInt,
    l(1).toInt,
    l(2).toInt,
    l(3).toInt,
    l(4).toInt,
    l(5).toInt,
    l(6).toDouble,
    l(7).toInt,
    l(8).toInt,
    l(9).toInt,
    l(10).toInt,
    l(11).toInt,
    l(12).toInt,
    l(13).toInt,
    l(14).toInt,
    l(15).toInt,
    l(16).toInt,
    l(17).toInt,
    l(18).toInt,
    l(19).asInstanceOf[String],
    l(20).toInt,
    l(21).toDouble.toInt
  )

  case class Transaction(DeviceOS: Int = 0,
                         Browser: Int = 0,
                         DeviceType: Int = 0,
                         ScreenResolution: Int = 0,
                         DeviceInfo: Int = 0,
                         uid: Int = 0,
                         TransactionAmt: Double = 0,
                         ProductCD: Int = 0,
                         isFraud: Int = 0,
                         card1: Int = 0,
                         card2: Int = 0,
                         card3: Int = 0,
                         card4: Int = 0,
                         card5: Int = 0,
                         card6: Int = 0,
                         region: Int = 0,
                         country: Int = 0,
                         R_emaildomain: Int = 0,
                         P_emaildomain: Int = 0,
                         TransactionDT: String = "",
                         TransactionID: Int = 0,
                         D1: Int = 0
                        )
    //implicit def tupleToTransactions(t: TransactionSerialized): TransactionSerialized = (t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13, t._14, t._15, t._16, t._17, t._18, t._19, t._20, t._21, t._22)



}
