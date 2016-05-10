package MlLibTest.test_Hello

import org.apache.spark._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by zhangwj on 16-5-10.
 */
class MyPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val domain = new java.net.URL(key.toString).getHost()
    val code = (domain.hashCode % numPartitions)
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }

  override def equals(other: Any): Boolean = other match {
    case iteblog: MyPartitioner =>
      iteblog.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

object testPartitioner {
  val conf = new SparkConf().setAppName("testPartition").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def testHashPartitioner(){
    val hashPartitioner = new HashPartitioner(3)
    val rdd = sc.parallelize(1.to(10)).map(x => (x, x)).partitionBy(hashPartitioner)  //只有(k,v)类型才能调用partitionBy
    rdd.mapPartitionsWithIndex{ (idx, iter) => //iter是一个集合（一个分区内的）
      val candidates = ArrayBuffer.empty[(Int, Int)]
      while (iter.hasNext) {
        val item = iter.next()
        val pairData = (item._1,item._2)
        candidates+=pairData
      }
      Iterator((idx,candidates))  //sample 采集的样本，n就是sampleSizePerPartition
    }.collect().foreach(println)
    //可以看到输出的结果中partition编号分别为0,1,2
    //0号分区中没有元素，1号partition中2个元素，2号分区中3个元素，所以hashPartition各个分区中元素个数分布不均
  }

  def testRangePartition(): Unit ={
    val rdd = sc.parallelize(1.to(10)).map(x => (x, x))
    // We have different behaviour of getPartition for partitions with less than 1000 and more than
    // 1000 partitions.
    val rangePartitioner = new RangePartitioner[Int,Int](5, rdd,ascending = false)
    rdd.partitionBy(rangePartitioner).mapPartitionsWithIndex{ (idx, iter) => //iter是一个集合（一个分区内的）
      val candidates = ArrayBuffer.empty[(Int, Int)]
      while (iter.hasNext) {
        val item = iter.next()
        val pairData = (item._1,item._2)
        candidates+=pairData
      }
      Iterator((idx,candidates))  //sample 采集的样本，n就是sampleSizePerPartition
    }.collect().foreach(println)
    //可以看到每个分区中的数量都是两个
    //分区之间是有序的
  }

  def userDefinePartition(): Unit ={
    val rdd = sc.parallelize(Seq("http://www.jd.com/a","http://www.jd.com/b","http://www.jd.com/c","http://www.google.com/a","http://www.google.com/b","http://www.baidu.com")).map(x => (x, x))
    val myPartitioner = new MyPartitioner(3)
    rdd.partitionBy(myPartitioner).mapPartitionsWithIndex{ (idx, iter) => //iter是一个集合（一个分区内的）
      val candidates = ArrayBuffer.empty[(String, String)]
      while (iter.hasNext) {
        val item = iter.next()
        val pairData = (item._1,item._2)
        candidates+=pairData
      }
      Iterator((idx,candidates))  //sample 采集的样本，n就是sampleSizePerPartition
    }.collect().foreach(println)
  }

  def main(args: Array[String]): Unit = {
//    testRangePartition
//    testHashPartitioner
    userDefinePartition()
  }

}
