package StreamingTest.test001

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by zhangwj on 16-3-4.
 */
class testUpdateStateByKey {

  val conf = new SparkConf().setAppName("testStreaming").setMaster("local[2]").setSparkHome(System.getenv("SPARK_HOME"))

  def updateFunction(currValues: Seq[Int], prevValueState: Option[Int]): Option[Int] = {
    //通过Spark内部的reduceByKey按key归约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
    val currentCount = currValues.sum
    // 已累加的值
    val previousCount = prevValueState.getOrElse(0)
    // 返回累加后的结果，是一个Option[Int]类型
    Some(currentCount + previousCount)
  }

  def main(args: Array[String]) {
    val ssc = new StreamingContext(conf,Seconds(10))
    ssc.checkpoint("target/tmp/spark/checkpoint")

    val lines = ssc.socketTextStream("localhost",9999)  //返回类型 ReceiverInputDStream->ReceiverInputDStream->InputDStream->DStream
    val words = lines.flatMap(_.split(" "))   //lines是多行，flatMap is a one-to-many DStream operation， split只能对每行，要放在flatMap中
    val pairs = words.map(w=>(w,1))

    /*  每次的结果都累加  */
    val totalCounts = pairs.updateStateByKey[Int](updateFunction _)
    totalCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}
