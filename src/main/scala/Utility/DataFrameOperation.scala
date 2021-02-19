package Utility

import org.apache.spark.sql.DataFrame


trait habitual{
  def getHabitual()
}

object DataFrameOperation {

  implicit class ImplicitsDataFrameCustomOperation(base: DataFrame) extends habitual{

    def customOperation(): DataFrame = {
      //business logic
      base
    }

    override def getHabitual(): Unit = ???
  }
}
