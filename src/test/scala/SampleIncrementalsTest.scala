/**
  * Created by pavachan on 6/19/2017.
  */
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

/**
  * Created by pavachan on 6/16/2017.
  */
class SampleIncrementalsTest extends FunSuite with DataFrameSuiteBase {

  test ("Counts") {

    val sqlCtx = sqlContext

    val mprop = new java.util.Properties()
    mprop.put("user", "root")
    mprop.put("password", "pavan")
    mprop.put("driver", "com.mysql.jdbc.Driver")
    mprop.put("lowerBound", "1")
    mprop.put("upperBound", "10000")
    mprop.put("numPartitions", "2")
    mprop.put("fetchsize","10000")

    val employeesUrl = "jdbc:mysql://localhost/employees"

    val employeesDF = sqlCtx.read.jdbc(employeesUrl, "employees", mprop)


    val employeePhoenixDF = sqlCtx.read.format("org.apache.phoenix.spark").
      option("table", "TEST.EMPLOYEES").
      option("zkUrl", "localhost").load()

    assert(employeesDF.count()=== employeePhoenixDF.count())

  }


}
