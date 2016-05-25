package MlTest.test001_baseTest

import breeze.linalg.sum
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-5-25.
 */
object testDataFrame {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test DataFrame")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._
    val dataDF = hiveContext.table("test").where($"col1"==="aaa").na.drop
    // where == filter
    // $ 为sqlContext提供的一个运算符，用来将col name转换为Column, 使用$的方式需要 import hiveContext.implicits._
    // na.drop 去除有缺失值的列。
    dataDF.show   //显示表的数据，默认显示前20行

    dataDF.printSchema  //查看表的schema

    //使用聚合函数        先按照某列值统计出出现的个数，再按从高到低的方式输出
    import org.apache.spark.sql.functions._
    val aggData = dataDF.groupBy("aaa").agg(count("bbb")) //agg默认是对所有聚合,先groupBy则是对aaa列的不同值进行聚合
    val sortData = aggData.sort($"bbb".desc)   //.asc升序，.desc降序
  }
}
