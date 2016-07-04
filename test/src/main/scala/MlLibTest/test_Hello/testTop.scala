package MlLibTest.test_Hello

import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-7-4.
 */
object testTop {

  val conf = new SparkConf().setAppName("MyTest").setMaster("local[2]").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)
  var data = sc.makeRDD(Array(("A1",1),("A2",20),("A3",3),("A4",100),("A5",200),("A6",3),("B1",1),("B2",2),("C1",1),("B3",1000),("B4",802),("B5",600),("C2",601)))

  def test0(): Unit ={
    data.top(5).foreach(println)  //原始输出
    /*
    * (C2,601)
      (C1,1)
      (B5,600)
      (B4,802)
      (B3,1000)
    * */
  }

  def test1(): Unit ={
    val ord = Ordering.by[(String,Int), Int](_._2)  //显式定义Ordering
    data.top(5)(ord).foreach(println)
    /*
    * (B3,1000)
      (B4,802)
      (C2,601)
      (B5,600)
      (A5,200)
    * */
  }

  def test2(): Unit ={
    implicit val ord = Ordering.by[(String,Int), Int](_._2)  //定义隐式Ordering
    data.top(5).foreach(println)
    /*
    * (B3,1000)
      (B4,802)
      (C2,601)
      (B5,600)
      (A5,200)
    * */
  }

  def test3(): Unit ={
    implicit val KeyOrdering = new Ordering[(String,Int)] {     //完全自定义compare
      override def compare(x:(String,Int), y: (String,Int)): Int = {
        x._2 - y._2
      }
    }
    data.top(5).foreach(println)
  }

  def test4(): Unit ={
    val KeyOrdering = new Ordering[(String,Int)] {     //完全自定义compare，显式定义
      override def compare(x:(String,Int), y: (String,Int)): Int = {
        x._2 - y._2
      }
    }
    data.top(5)(KeyOrdering).foreach(println)   //显式调用
  }

  def main(args: Array[String]) {
//    test0
//    test1
//    test2
//    test3
      test4
  }
}
