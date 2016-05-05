package StreamingTest.test000_Hello

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-3-4.
 *   programming guid: https://spark.apache.org/docs/latest/streaming-programming-guide.html
 *   API Streaming Context: https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.StreamingContext
 */
object testHello {

  val conf = new SparkConf().setAppName("testStreaming").setMaster("local[2]").setSparkHome(System.getenv("SPARK_HOME"))
  //“local[n]” as the master URL, where n > number of receivers to run，至少一个线程用来接受，其他线程用来处理数据，所以不能用local/local[1]

  def testStreamingContext(): Unit ={

    //   StreamingContext   （1）
    //  从 SparkConf 创建 StreamingContext
    import org.apache.spark.streaming._
    val ssc = new StreamingContext(conf,Seconds(10))
//    ssc.checkpoint("hdfs://localhost:9000/spark/checkpoint")
    ssc.checkpoint("target/tmp/spark/checkpoint")

    // create a DStream that represents streaming data from a TCP source
    val lines = ssc.socketTextStream("localhost",9999)  //返回类型 ReceiverInputDStream->ReceiverInputDStream->InputDStream->DStream
    // Split each line into words
    val words = lines.flatMap(_.split(" "))   //lines是多行，flatMap is a one-to-many DStream operation， split只能对每行，要放在flatMap中


    // Count each word in each batch
    val pairs = words.map(w=>(w,1))


    /*   得到每次执行的结果   */
    val WordsCount = pairs.reduceByKey(_+_)
    WordsCount.print()


    /*  每次的结果都累加  */
    val totalCounts = pairs.updateStateByKey[Int](updateFunction _)
    totalCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()  // Wait for the computation to terminate

    /*   execute linux command: nc -lk 9999       */
  }

  def updateFunction(currValues: Seq[Int], prevValueState: Option[Int]): Option[Int] = {
    //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
    val currentCount = currValues.sum
    // 已累加的值
    val previousCount = prevValueState.getOrElse(0)
    // 返回累加后的结果，是一个Option[Int]类型
    Some(currentCount + previousCount)
  }

  def testCreateStreamingContext(): Unit ={
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("target/tmp/spark/checkpoint")

    //   StreamingContext   （2）
    // A StreamingContext object can also be created from an existing SparkContext object.  从 SparkContext 创建StreamingContext
    val ssc2 = new StreamingContext(sc,Seconds(3))

  }


  def main(args: Array[String]) {

    testStreamingContext()

  }
}
