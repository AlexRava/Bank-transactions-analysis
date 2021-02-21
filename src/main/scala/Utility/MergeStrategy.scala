package Utility

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

trait MergeStrategy {

  def merge(dataframes: mutable.Map[String,DataFrame]):DataFrame

}
