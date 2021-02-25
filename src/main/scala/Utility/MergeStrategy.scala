package Utility

import Sources.KafkaSources.InputSource
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * An utility Object with some default dataframes merge strategy
  */
object MergeStrategy {

  /**
    * A simple merge strategy
    * @param dataframes A collection of dataframe, each one is define by the source's name.
    * @return a final dataframe that's the final result of a simple merging strategy.
    */
  def simpleStrategy(dataframes: mutable.Map[String,DataFrame]):DataFrame = dataframes.get(InputSource.topic).get

}
