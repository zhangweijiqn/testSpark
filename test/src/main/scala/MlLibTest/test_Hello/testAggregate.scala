package MlLibTest.test_Hello

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-5-23.
 */
object testAggregate {
  val conf = new SparkConf().setAppName("MyTest").setMaster("local[2]").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val data = sc.parallelize(List (1 ,2 ,3 ,4 ,5 ,6) , 2)

    def seqOP(a:Double, b:Int) : Double = {
      println("seqOp: " + a + "\t" + b)
      math.max(a,b)
    }

    def combOp(a:Double, b:Double): Double = {
      println("combOp: " + a + "\t" + b)
      a + b
    }
    val aggregateData = data.aggregate(0.000)(seqOP,combOp)   //第一个参数为初始值，传入的初始值可以是一个不同于rdd的类型。
//    println(aggregateData)

    val reduceData = data.reduce((a,b)=>a+b)  //aggregate，reduce操作均为action
    println(reduceData)

  }
}
