package Utility

import org.apache.spark.sql.DataFrame


trait CustomFeature{
  def computeNewFeature(computation: DataFrame => DataFrame): DataFrame
}

trait DefinedFeature extends CustomFeature {


  def addColHabitualBehaviour():DataFrame

  def addColMeanAmountForPeriod():DataFrame
}

trait Prediction{
  def addPrediction():DataFrame
}

object DataFrameExtension {

  implicit class ImplicitsDataFrameCustomOperation(base: DataFrame) extends DefinedFeature {

    override def computeNewFeature(computation: DataFrame => DataFrame): DataFrame = computation(base)

    override def addColHabitualBehaviour(): DataFrame = base

    override def addColMeanAmountForPeriod(): DataFrame = base


  }

  implicit class ImplicitDataframeWithModelPrediction(base: DataFrame) extends Prediction{
    override def addPrediction(): DataFrame = base
  }
}
