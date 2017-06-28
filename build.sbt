name := "GNISMorphTest"

version := "1.0"

scalaVersion := "2.10.6"

unmanagedJars in Compile ++= Seq(file("lib/phoenix-4.7.0-HBase-1.1-client.jar"))

libraryDependencies ++= Seq(

  "org.apache.spark" % "spark-core_2.10" % "1.6.1" ,
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.1" ,
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.3",
  "org.apache.phoenix" % "phoenix-spark" % "4.7.0-HBase-1.1",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.apache.spark" % "spark-hive_2.10" % "1.6.1",
  "com.databricks" % "spark-csv_2.11" % "1.2.0",
  "com.typesafe" %% "scalalogging-slf4j" % "1.1.0",
  "mysql" % "mysql-connector-java" % "5.1.42",
  "com.holdenkarau" % "spark-testing-base_2.10" % "1.6.1_0.4.4" % "test",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.4",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.7.4"


)