package MlLibTest.test001_Data

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhangweijian01 on 2016/12/19.
  */
object testReadFile {
  //设置运行环境
  val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
  val sc = new SparkContext(conf)
  def main(args: Array[String]): Unit = {
    val data = sc.textFile("test/src/main/resources/data/train.txt,test/src/main/resources/data/train.txt") //两个文件
    println(data.count())
  }
}
