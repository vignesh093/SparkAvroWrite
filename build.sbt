import sbtassembly.MergeStrategy

name := "SparkAvroWrite"
version := "1.0"
scalaVersion := "2.11.11"
autoScalaLibrary := false

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided"

libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.0.0"

libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "3.0.0"

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class")         => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
   oldStrategy(x)
}