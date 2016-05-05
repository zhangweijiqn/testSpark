package MlLibTest.test_Hello

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
/**
 * Created by zhangwj on 16-2-16.
 */
object Mytest {

  //获取spark RDD[Vector]
  val conf = new SparkConf().setAppName("MyTest").setMaster("local[2]").setSparkHome(System.getenv("SPARK_HOME"))
  //setMaster设置spark master节点，  setSparkHome设置spark_home，submit的时候会根据spark_home下配置文件信息将jar包分发到worker节点
  //The appName parameter is a name for your application to show on the cluster UI. master is a Spark, Mesos or YARN cluster URL, or a special “local[*]” string to run in local mode.
  val sc = new SparkContext(conf)

  def testGetRDDVector(): Unit ={
    val data = sc.textFile("src/main/resources/test.txt").map(_.split(" ")).map(_.map(_.toDouble))
    data.foreach(_.foreach(println))
    val observations: RDD[Vector] = data.map(x => Vectors.dense(x))   //需要引入mllib的vector，否则会使用scala vector，编译不通过
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
  }

  def testRDDCartesian(): Unit ={
    val rddA = sc.parallelize(Seq(1,2,3,4,5))
    //如果要求rddA每两个元素之间的相关性，正常需要rddA.map( x=>rddA.map(y=>x*y) )，但是这种rdd嵌套rdd是不允许的
    //转换一下思路
    val A = rddA.cartesian(rddA)
    val upTriagleMatirx = A.filter(x=>x._1<x._2).foreach(println) //只保留上三角的元素
  }


  def main(args:Array[String]) {
//    testGetRDDVector
    testRDDCartesian
  }
}
