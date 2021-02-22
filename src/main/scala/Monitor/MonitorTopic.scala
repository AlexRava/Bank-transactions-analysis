package Monitor

import Sources.KafkaSource
import Streams.{StreamUtility}

/**
  * Thank's to this abstraction it's possible to monitor a Topic
  */
trait MonitorTopic {
  def monitor()
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

  override def monitor() = StreamUtility.printInStdOut(source.readFromSource())
}
