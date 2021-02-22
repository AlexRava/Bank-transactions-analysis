package Utility

import Sources.KafkaSources.InputSource
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object MergeStrategy {

  /**
    * A simple merge strategy
    * @param dataframes A collection of dataframe, each one is define by the source's name.
    * @return a final dataframe that's the final result of a custom merging strategy
    */
  def simpleStrategy(dataframes: mutable.Map[String,DataFrame]):DataFrame = dataframes.get(InputSource.topic).get

}
