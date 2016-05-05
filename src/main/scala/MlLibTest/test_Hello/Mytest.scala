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


  def testCombineByKey(): Unit ={

    /*val createCombiner = (v: Double) => {
      val queue = new BoundedPriorityQueue[Double](100);
      queue+=(v)
      queue
    }

    val mergeValue = (q:BoundedPriorityQueue[Double],v: (Long,Double)) => {
      q += v._2
    }
    val mergeCombiners = (q1: BoundedPriorityQueue[Double], q2:BoundedPriorityQueue[Double]) => {
      q1.++=(q2)
      q1
    }

    val testData = sc.parallelize(Seq((1L,0.1),(1L,0.2),(1L,0.3),(2L,0.2),(2L,0.5)))
    testData.combineByKey(createCombiner,mergeValue,mergeCombiners,10)*/
    var rdd1 = sc.makeRDD(Array(("A",1),("A",20),("A",3),("A",100),("A",200),("A",3),("B",1),("B",2),("C",1),("B",1000),("B",802),("B",600),("C",601)))
    val rdd2 = rdd1.combineByKey((v : Int) => v + "_",
      (c : String, v : Int) => c + "@" + v,
      (c1 : String, c2 : String) => c1 + "$" + c2)
    rdd2.collect.foreach(println)

    val rdd3 = rdd1.combineByKey(
      (v : Int) => List(v),
      (c : List[Int], v : Int) => v :: c,
      (c1 : List[Int], c2 : List[Int]) => c1 ::: c2
    )
    rdd3.collect.foreach(println)

    val createCombiner = (v: Int) => {
      val queue = new BoundedPriorityQueue[Int](2)(Ordering.Option.reverse);
      queue+=(v)
      queue
    }

    val mergeValue = (q:BoundedPriorityQueue[Int],v: (Int)) => {
      q += v
    }
    val mergeCombiners = (q1: BoundedPriorityQueue[Int], q2:BoundedPriorityQueue[Int]) => {
      q1.++=(q2)
      q1
    }
    val rdd4 = rdd1.combineByKey(
      createCombiner,
      mergeValue,
      mergeCombiners,
    10
    )
    println("-------------------------------")
    rdd4.collect().foreach(x=>{println(x._1+":"+x._2.toArray.mkString(","))})
  }

  def main(args:Array[String]) {
//    testGetRDDVector
//    testRDDCartesian
    testCombineByKey()
  }
}
