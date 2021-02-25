package Monitor

import App.Application.spark
import Data.{Prediction, TransactionTransformed}
import Sources.KafkaSource
import Utility.StreamUtility
import org.apache.spark.sql.functions.from_json

/**
  * Thank's to this abstraction it's possible to monitor a Topic
  */
trait MonitorTopic {
  def monitor()

  def prediction()
}

/**
  * Factory for a Monitoring topic.
  */
object MonitorTopic{
  def fromTopicSource(s:KafkaSource): MonitorTopicImpl = new MonitorTopicImpl(s)
}

/**
  * implements a logic for monitoring a topic.
  * @param source Topic to be monitored.
  */
class MonitorTopicImpl(source : KafkaSource) extends MonitorTopic {

  import spark.implicits._

  override def monitor() = StreamUtility.printInStdOut(source.readFromSource())

  override def prediction() = source.readFromSource()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String,String)]
    .select($"key",from_json($"value",Prediction.PredictionSchema) as "data")
    .select($"data.uid" as "user",$"data.TransactionID",$"data.prediction" as "Prediction 0:Ok - 1:Fraud")
    .writeStream
    .outputMode("update")
    .format("console")
    .start()

}
