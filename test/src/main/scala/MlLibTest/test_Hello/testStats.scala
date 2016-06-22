package MlLibTest.test_Hello

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-6-7.
 */
object testStats {
  val conf = new SparkConf().setAppName("MyTest").setMaster("local[2]").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val data = sc.parallelize(Seq((1,1.0),(1,2.2),(2,2.1),(2,3.5),(3,3.9)))

    // test countByKey
    val countData = data.countByKey()//根据key的数量查看是否有数据倾斜的情况。
    countData.foreach(println)

    // test count by value
    val dataCount = data.countByValue()   //计算每个RDD元素出现的次数，这里RDD元素类型为[int,int]，只支持返回结果小的情况。大的情况用reduceByKey
    dataCount.foreach(println)

    // test stats
    val statsData = data.map(_._2).stats()
    println(statsData.toString())
    //(count: 5, mean: 2.200000, stdev: 0.748331, max: 3.000000, min: 1.000000)

    // 手动过滤缺失值
    val NaNData = data ++ sc.parallelize(Seq((4,Double.NaN)))
    import java.lang.Double.isNaN
    val filterData = NaNData.map(_._2).filter(!isNaN(_))
    filterData.foreach(println)
  }
}
