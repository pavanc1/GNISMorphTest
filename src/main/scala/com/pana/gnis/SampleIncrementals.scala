package com.pana.gnis

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by pavachan on 6/16/2017.
  */
object SampleIncrementals {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SampleIncrementals")
    val sc = new SparkContext(conf)
    val sparkSQLContext = new SQLContext(sc)

    val mprop = new java.util.Properties()
    mprop.put("user", "root")
    mprop.put("password", "pavan")
    mprop.put("driver", "com.mysql.jdbc.Driver")
    mprop.put("lowerBound", "1")
    mprop.put("upperBound", "10000")
    mprop.put("numPartitions", "2")
    mprop.put("fetchsize","10000")

    val employeesUrl = "jdbc:mysql://localhost/employees"

//    val employeesQry = "select emp_no, birth_date, first_name, last_name, gender, hire_date from employees"

    val employeesDF = sparkSQLContext.read.jdbc(employeesUrl, "employees", mprop)

    employeesDF.write.mode(SaveMode.Overwrite).format("org.apache.phoenix.spark")
      .option("table", "TEST.EMPLOYEES")
      .option("zkUrl", "localhost")
      .option("phoenix.connection.autoCommit", "true")
      .option("phoenix.mutate.batchSize", "100")
      .save()

    employeesDF.printSchema()



  }


}
