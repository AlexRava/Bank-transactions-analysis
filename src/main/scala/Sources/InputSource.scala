package Sources

import App.Application.spark

object InputSource extends Source{

  import spark.implicits._

  def readFromSource() = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "input-topic")
    .load()

}
