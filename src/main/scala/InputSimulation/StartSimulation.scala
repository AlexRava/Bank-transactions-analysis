package InputSimulation


object StartSimulation extends App{

  UsersSimulation.init()
  UsersSimulation.start( sourceFactory.readFromCSV("C:\\Users\\Alex\\Desktop\\Data_Bank\\incoming_data.csv") )

}
