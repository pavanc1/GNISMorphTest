package com.pana.gnis

import com.typesafe.scalalogging.slf4j.Logger
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


/**
  * Created by pavachan on 6/15/2017.
  */
object GNISDataIngestion extends App{

  val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  final val MSG_GNIS_DEVICE_FETCH = "Fetching SYNC_GNIS.DEVICE table data"
  final val MSG_GNIS_INSERT_NETWORK_DETAILS = "Inserting GNIS source data to NETWORK_DETAILS hbase table"
  final val MSG_GNIS_NODE_FETCH = "Fetching SYNC_GNIS.EXTRACT_NODE, SYNC_GNIS.EXTRACT_JOB and SYNC_GNIS.NODE tables data"
  final val MSG_GNIS_INSERT_NODE_STATUS = "Inserting GNIS NODE and EXTRACTJOB data to NODE_STATUS hbase table"

    val mprop = new java.util.Properties()
  mprop.put("user", "root")
  mprop.put("password", "pavan")
  mprop.put("driver", "com.mysql.jdbc.Driver")
  mprop.put("lowerBound", "1")
  mprop.put("upperBound", "10000")
  mprop.put("numPartitions", "2")
  mprop.put("fetchsize","10000")

  val conf = new SparkConf().setMaster("local[*]").setAppName("GNISDataIngestion")
  val sc = new SparkContext(conf)
  val ssc = new SQLContext(sc)

  logger.info("Spark context initialized")

  val device = ssc.read.format("com.databricks.spark.csv").option("header", "true").load("C:\\GNISSampleData\\device.csv")
  device.registerTempTable("device")

  logger.info("device loaded")

  val extractJob = ssc.read.format("com.databricks.spark.csv").option("header", "true").load("C:\\GNISSampleData\\extract_job.csv")
  extractJob.registerTempTable("extractJob")

  logger.info("extract loaded")

  val extractNode = ssc.read.format("com.databricks.spark.csv").option("header", "true").load("C:\\GNISSampleData\\extract_node.csv")
  extractNode.registerTempTable("extractNode")

  logger.info("extractnode loaded")

  val node = ssc.read.format("com.databricks.spark.csv").option("header", "true").load("C:\\GNISSampleData\\node.csv")
  node.registerTempTable("node")

  logger.info("node loaded")
//
//  println(device.count()+"-------CSV---------->")
//  device.printSchema()



//  val deviceParDF = ssc.read.parquet("C:\\GNISSampleData\\GNIS_DATA\\device")
//  deviceParDF.registerTempTable("device")
//  val extractJobDF = ssc.read.parquet("C:\\GNISSampleData\\GNIS_DATA\\EXTRACT_JOB")
//  extractJobDF.registerTempTable("extractJob")
//
//  val extractNodeDF = ssc.read.parquet("C:\\GNISSampleData\\GNIS_DATA\\EXTRACT_NODE")
//  extractNodeDF.registerTempTable("extractNode")
//
//  val nodeDF = ssc.read.parquet("C:\\GNISSampleData\\GNIS_DATA\\node")
//  nodeDF.registerTempTable("node")

  val gnisNodeDF = ssc.sql("SELECT ND.ID AS NODEID, EN.NODE_NUMBER AS NODENUMBER, EN.EXTRACT_JOB_ID AS EXTRACTJOBID, EJ.EXTRACT_TIMESTAMP AS EXTRACTTIMESTAMP " +
    "FROM node ND, extractJob EJ, extractNode EN WHERE EN.NODE_NUMBER = ND.NODE_NUMBER AND EN.EXTRACT_JOB_ID = EJ.ID")

  println("---------------------------------")
  println(gnisNodeDF.count()+"---------------")
  println("----------------------------------")

  gnisNodeDF.registerTempTable("nodeStatusGNIS")

  val resultNodeStatusDF = ssc.sql("select NODEID, NODENUMBER, EXTRACTJOBID, EXTRACTTIMESTAMP from nodeStatusGNIS limit 100")

  resultNodeStatusDF.printSchema()

  resultNodeStatusDF.write.mode(SaveMode.Overwrite).format("org.apache.phoenix.spark")
    .option("table", "TEST.NODE_STATUS")
    .option("zkUrl", "localhost")
    .option("phoenix.connection.autoCommit", "true")
    .option("phoenix.mutate.batchSize", "100")
    .save()


}
