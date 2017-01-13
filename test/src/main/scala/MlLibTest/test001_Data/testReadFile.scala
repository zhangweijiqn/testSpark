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

  /*读取带有日期的文件
    1，定义一个path:String，从起始日期开始遍历到结束日期，每次构建一个日期文件路径加到path中，最终path含有多个文件
    2，像按月份选择，可以直接使用 "/path/file_201612*,/path/file_201701*"
  */
}
