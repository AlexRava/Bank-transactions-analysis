package Data

object DataObject {

  trait Creatable[T <: Creatable[T]] {
    val cs = this.getClass.getConstructors

    def apply(params: Seq[Any]) ={
      cs(0).newInstance(params map { _.asInstanceOf[AnyRef] } : _*).asInstanceOf[T]}
  }

  case class Transaction(DeviceOS: String = "",
                         Browser: String = "",
                         DeviceType: String = "",
                         ScreenResolution: String = "",
                         DeviceInfo: String = "",
                         uid: String = "",
                         TransactionAmt: String = "",
                         ProductCD: String = "",
                         isFraud: String = "",
                         card1: String = "",
                         card2: String = "",
                         card3: String = "",
                         card4: String = "",
                         card5: String = "",
                         card6: String = "",
                         region: String = "",
                         country: String = "",
                         R_emaildomain: String = "",
                         P_emaildomain: String = "",
                         TransactionDT: String = "",
                         TransactionID: String = "",
                         D1: String = ""

                        ) extends Creatable[Transaction]
}
