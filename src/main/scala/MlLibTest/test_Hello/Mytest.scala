package MlLibTest.test_Hello

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
/**
 * Created by zhangwj on 16-2-16.
 *
 * 测试 combineByKey (ReverseOrder)以及 BoundedPriorityQueue
 *
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

    var rdd1 = sc.makeRDD(Array(("A",1),("A",20),("A",3),("A",100),("A",200),("A",3),("B",1),("B",2),("C",1),("B",1000),("B",802),("B",600),("C",601)))

    /*  test1  */
    val rdd2 = rdd1.combineByKey((v : Int) => v + "_",
      (c : String, v : Int) => c + "@" + v,
      (c1 : String, c2 : String) => c1 + "$" + c2)
    rdd2.collect.foreach(println)

    /*  test2
    *  下面这个过程类似groupByKey的实现，List替换为CompactBuffer
    * */
    val rdd3 = rdd1.combineByKey(
      (v : Int) => List(v),
      (c : List[Int], v : Int) => v :: c,
      (c1 : List[Int], c2 : List[Int]) => c1 ::: c2
    )
    rdd3.collect.foreach(println)

    /*  test3  */
    val createCombiner = (v: Int) => {   //一个新出现的key先创建Combiner
      val queue = new BoundedPriorityQueue[Int](2)
      queue+=(v)
      queue
    }
    val mergeValue = (q:BoundedPriorityQueue[Int],v: (Int)) => {   //相同的key元素和已存在key的combiner进行merge
      q += v
    }
    val mergeCombiners = (q1: BoundedPriorityQueue[Int], q2:BoundedPriorityQueue[Int]) => {   //不同key的combiner之间进行merge
      q1.++=(q2)
      q1
    }
    val rdd4 = rdd1.combineByKey(
      createCombiner,
      mergeValue,
      mergeCombiners)
    println("-------------------------------")
    rdd4.collect().foreach(x=>{println(x._1+":"+x._2.toArray.mkString(","))})
  }

  def testCombineByKeyReverseOrder(): Unit ={
    implicit val KeyOrdering = new Ordering[Tuple2[Long,Double]] {
      override def compare(x:Tuple2[Long,Double], y: Tuple2[Long,Double]): Int = {
        (x._2*100000 - y._2*100000).toInt //乘的倍数决定了double运算的精度
      }
    }

    val createCombiner = (v: (Long,Double)) => {
      val queue = new BoundedPriorityQueue[(Long,Double)](2)(KeyOrdering.reverse);
      queue+=(v)
      queue
    }

    val mergeValue = (q:BoundedPriorityQueue[(Long,Double)],v: (Long,Double)) => {
      q += v
    }
    val mergeCombiners = (q1: BoundedPriorityQueue[(Long,Double)], q2:BoundedPriorityQueue[(Long,Double)]) => {
      q1.++=(q2)
      q1
    }

    val testData = sc.parallelize(Seq((1L,(1L,0.5)),(1L,(2L,0.2)),(1L,(3L,0.3)),(2L,(2L,0.2)),(2L,(3L,0.5))))
    val combineData = testData.combineByKey(createCombiner,mergeValue,mergeCombiners)
    combineData.collect().foreach(x=>{println(x._1+":"+x._2.toMap)})
  }

  def main(args:Array[String]) {
//    testGetRDDVector
//    testRDDCartesian
//    testCombineByKey()


  }
}
