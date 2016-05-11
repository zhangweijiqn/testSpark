package MlLibTest.test_Hello

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-5-11.
 * RDD groupBy
 */
object testGroupByKey {
  val conf = new SparkConf().setAppName("MyTest").setMaster("local[2]").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val data = sc.parallelize(Seq((1,1,1),(1,2,3),(2,2,2),(2,3,4),(3,3,3)))
    val groupData = data.groupBy(x=>x._2) //RDD groupBy
    groupData.foreach(println)
    // groupBy可以用在tuple上，参数指定按照第几个值排序即可。

  }
}
