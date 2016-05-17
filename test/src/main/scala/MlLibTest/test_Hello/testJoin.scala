package MlLibTest.test_Hello

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-5-11.
 * 只有kv数据有join操作
 */
object testJoin {
  val conf = new SparkConf().setAppName("MyTest").setMaster("local[2]").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)

  def testJoin(): Unit ={
    val rdd1 = sc.makeRDD(Array(("A",1),("A",20),("A",3),("A",100),("A",200),("A",3),("B",1),("B",2),("C",1),("B",1000),("B",802),("B",600),("C",601)))
    val rdd2 = sc.makeRDD(Array(("A",112),("A",120),("A",113),("A",1001),("A",2001),("A",311),("B",11),("B",12),("C",11),("B",10001),("B",8021),("B",6001),("C",6011)))
    val joinRDD =  rdd1.join(rdd2)  //join后的类型为RDD[String,(Int,Int)],类似于数据库中join操作，RDD每个kv是一条记录，v和另一个RDD的所有v组合 ==相同key内value笛卡尔乘积
    joinRDD.foreach(x=>println(x._1+":"+x._2))
    println("count:"+joinRDD.count()) //65
  }

  def testCartesian(): Unit ={
    val rdd1 = sc.makeRDD(Array(("A",1),("A",20),("A",3),("A",100),("A",200),("A",3),("B",1),("B",2),("C",1),("B",1000),("B",802),("B",600),("C",601)))
    val rdd2 = sc.makeRDD(Array(("A",112),("A",120),("A",113),("A",1001),("A",2001),("A",311),("B",11),("B",12),("C",11),("B",10001),("B",8021),("B",6001),("C",6011)))
    val joinRDD =  rdd1.cartesian(rdd2)  //join后的类型为RDD[(String,Int),(String,Int)],kv和另一个RDD的所有kv组合 ==笛卡尔乘积
    joinRDD.foreach(x=>println(x._1+":"+x._2))
    println("count:"+joinRDD.count()) //169
  }

  def main(args: Array[String]) {
    testJoin()
//    testCartesian()
  }
}
