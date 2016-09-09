package SQLTest.test003_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import SQLTest.test003_sql.testUDF

/**
 * Created by zhangwj on 16-3-1.
 *
 *
 */
object testSql {

  val conf = new SparkConf().setAppName("testSQL")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)//需要添加hive依赖包，否则会报class not found的错误
  //可以在spark-shell中调试，默认的sqlContext使用的就是HiveContext

  def test1(): Unit ={

    hiveContext.sql("CREATE TABLE IF NOT EXISTS src (key string, value STRING)")
    hiveContext.sql("LOAD DATA LOCAL INPATH '/home/zhangwj/MyProjects/testProjects/TestLanguageTech/TestSpark/src/main/resources/sample.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
//    sqlContext.sql("FROM src SELECT key, value").collect().foreach(println)
  }

  case class Employees(name:String,salary:Double);

  def test2(): Unit ={
    import hiveContext.implicits._
    val data = sc.textFile("hdfs://localhost:9000/user/hive/warehouse/testdb_001.db/employees")
      .map(_.split("\t")).map(r=>Employees(r(0),r(1).toDouble))
      .toDF()
    data.registerTempTable("employees")
    val emplyee_Number = hiveContext.sql("select count(*) from employees")
//    emplyee_Number.foreach(println)

  }

  def UDFtest()={
    /* UDF */
    hiveContext.udf.register("isExist", testUDF.isExist)
    hiveContext.sql("select isExist(features) from tableAAa where ")
  }

  def main(args: Array[String]) {

    test1
    test2
    UDFtest

  }
}
