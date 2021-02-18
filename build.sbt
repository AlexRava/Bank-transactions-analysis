name := "Kafka_SparkStreaming"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "3.0.1",
  "joda-time" % "joda-time" % "2.10.10",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.0", //% "provided",
  "org.apache.spark" %% "spark-sql" % "3.1.0",
  // "org.apache.spark" % "spark-streaming_2.12" % "3.0.1" ,//% "provided",
  "org.apache.spark" %% "spark-streaming" % "3.1.0",// % "provided",
  //"com.tuplejump" %% "kafka-connect-cassandra" % "0.0.7",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.1.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.7.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.14.0",
  "com.github.tototoshi" %% "scala-csv" % "1.3.6",
  "com.github.jnr" % "jnr-posix" % "3.1.4"

)