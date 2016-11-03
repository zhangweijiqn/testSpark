package SQLTest.test001_DataFrame

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 *  Created by zhangwj on 16-3-1.
 *  https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame
 */
// case class 要放在object外面，否则报错
case class Person(name: String, age: Long)

object testDataFrame {

  val conf = new SparkConf().setAppName("hello").setMaster("local[2]").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)

  //The entry point into all functionality in Spark SQL is the SQLContext class, or one of its descendants. To create a basic SQLContext, all you need is a SparkContext.0
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  def test1() ={
    //create dataframe
//    val df = sqlContext.read.json("src/main/resources/people.json")
    val df = sqlContext.read.json("src/main/resources/people.json")

    //DataFrame Operations

    // Displays the content of the DataFrame to stdout
    df.show()

    // Print the schema in a tree format
    df.printSchema()

    // Select only the "name" column
    df.select("name").show()

    // Select everybody, but increment the age by 1
    df.select(df("name"), df("age") + 1).show()

    // The following are equivalent:
    df.selectExpr("colA", "colB as newName", "abs(colC)")

    // Select people older than 21
    df.filter(df("age") > 21).show()

    // Count people by age
    df.groupBy("age").count().show()

    val tableRDD = df.rdd.map(x=>(x(0),x.getString(1)))
    tableRDD.foreach(println)

    sqlContext.table("gdm.gdm_m04_ord_det_sum").where($"dp"==="HISTORY" and $"dt">="2015-06-14").select("user_log_acct").distinct.count

  }

  def test2(): Unit ={
    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
    val path = "test/src/main/resources/people.json"
    val people = sqlContext.read.json(path).as[Person]  //得到的是DataSet
    people.show
  }

  def test3(): Unit ={
    val tab = sqlContext.table("tmp.users__profile")
//    获取列名
    val column_names = tab.columns //Array(user_log_acct, cpp_addr_county, cpp_addr_province...
//    获取列名及类型
    val column_KT = tab.dtypes  //Array[(String, String)] = Array((user_log_acct,StringType), (cpp_addr_county,StringType))

  }


  def main(args: Array[String]) {

//    test1
    test2

  }
}
