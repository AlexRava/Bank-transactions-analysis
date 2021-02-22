import Sources.KafkaSource
import Streams.{StreamUtility, StreamingFlow}
import org.apache.spark.sql.DataFrame


trait MonitorTopic {
  def printData()
}

object MonitorTopic{
  def fromTopicSource(s:KafkaSource): MonitorTopicImpl = new MonitorTopicImpl(s)
}

class MonitorTopicImpl(source : KafkaSource) extends MonitorTopic {

  override def printData(): Unit = StreamUtility.printInStdOut(source.readFromSource())
}
