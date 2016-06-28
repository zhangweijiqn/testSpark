package MlTest.test001_baseTest
/*
* https://spark.apache.org/docs/latest/sql-programming-guide.html#dataframe-operations
* */

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-5-25.
 */
object testDataFrame {

  val conf = new SparkConf().setAppName("test DataFrame")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)

  def test(): Unit ={
    //implicit transform
    val hiveContext = new HiveContext(sc)

    implicit def execSql(sql:String):DataFrame={
      hiveContext.sql(sql)
    }

    val a ="select * from gdm.gdm_m03_item_sku_da limit 10"
    a.printSchema()
  }

  def main(args: Array[String]) {



    /*  basic  */
    import hiveContext.implicits._
    val dataDF = hiveContext.table("test").where($"col1"==="aaa").na.drop
    // where == filter
    // $ 为sqlContext提供的一个运算符，用来将col name转换为Column, 使用$的方式需要 import hiveContext.implicits._
    // na.drop 去除有缺失值的列。
    dataDF.show   //显示表的数据，默认显示前20行
    dataDF.printSchema  //查看表的schema
    dataDF.select("name").show()
    dataDF.select($"colA", $"colB" + 1)
    dataDF.select($"colA", $"colB" + 1)
    dataDF.groupBy("age").count().show()

    // The following are equivalent:
    dataDF.selectExpr("colA", "colB as newName", "abs(colC)")
    dataDF.select(expr("colA"), expr("colB as newName"), expr("abs(colC)"))

    /* dataFrame map  group agg  sort*/
    val transRDD = dataDF.map(x=>(x.getAs[String]("user_log_acct"),x.getAs[String]("cate"),x.getAs[Seq[String]]("features"),x.getAs[String]("dt"),x.getAs[String]("type")))
    //DataFrame map 返回 RDD
    val groupUsers = transRDD .toDF.groupBy($"_2").agg(count($"_2")).sort($"count(_2)".desc)
    // agg可以使用一些聚合函数
    //toDF后列名为_1,_2,...，聚合后列名为 count(_2)


    //使用聚合函数实例        先按照某列值统计出出现的个数，再按从高到低的方式输出
    import org.apache.spark.sql.functions._
    val aggData = dataDF.groupBy("aaa").agg(count("bbb")) //agg默认是对所有聚合,先groupBy则是对aaa列的不同值进行聚合
    val sortData = aggData.sort($"bbb".desc)    //.asc升序，.desc降序


    /* describe求统计信息  */
    // Computes statistics for numeric columns, including count, mean, stddev, min, and max.
    // If no columns are given, this function computes statistics for all numerical columns.
    dataDF.describe()
    // df.describe("age", "height").show()



    val newDF = dataDF.withColumn("aaa", lit("bbb"))  //df增加一个新列
    //withColumn Returns a new DataFrame by adding a column or replacing the existing column that has the same name.



  }
}
