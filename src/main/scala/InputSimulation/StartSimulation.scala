package InputSimulation


object StartSimulation extends App{

  val source : DataSource[Seq[String]] = sourceFactory.readFromCSV("C:\\Users\\Alex\\Desktop\\Data_Bank\\incoming_data.csv")

  UsersSimulation.init()
  UsersSimulation.start( source )

}
