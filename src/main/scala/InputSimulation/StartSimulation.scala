package InputSimulation


object StartSimulation extends App{

  val source : DataSource[Seq[String]] = sourceFactory.readFromCSV("C:\\Users\\Alex\\Desktop\\Fraud_Historical_data\\historical_data.csv")

  UsersSimulation.wait(0.7)
  UsersSimulation.init() // check how: default value, e/0 parametri opzionali posso non specificare nessun valore ?
  UsersSimulation.start( source )

}
