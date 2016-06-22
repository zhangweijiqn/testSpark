package MlLibTest.test_Hello

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-5-22.
 */
object testReduceByKey {
  val conf = new SparkConf().setAppName("MyTest").setMaster("local[2]").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val data = sc.parallelize(Seq((1,1),(1,2),(2,2),(2,3),(3,3)))
    data.coalesce(1).saveAsTextFile("test/target/test/data")  //rdd写到一个文件里，多个分区会写多个文件

    // test countByKey
    val countData = data.countByKey()//根据key的数量查看是否有数据倾斜的情况。
    countData.foreach(println)

    // test reduceByKey
    val reduceData = data.reduceByKey((a,b)=>a+b,1000) //RDD groupBy,设置并行度（也就是task的数量，默认200，在数量小的情况下设置大了反而影响性能）
    //1000，该参数就设置了这个shuffle算子执行时shuffle read task的数量。

    reduceData.foreach(println)
    // groupBy可以用在tuple上，参数指定按照第几个值排序即可。

    // test count by value
    val dataCount = data.countByValue()   //计算每个RDD元素出现的次数，这里RDD元素类型为[int,int]，只支持返回结果小的情况。
    dataCount.foreach(println)

    // test stats
    val statsData = data.map(_._2).stats()
    println(statsData.toString())
    //(count: 5, mean: 2.200000, stdev: 0.748331, max: 3.000000, min: 1.000000)
  }
}
