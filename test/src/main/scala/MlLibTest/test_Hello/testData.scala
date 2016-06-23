package MlLibTest.test_Hello

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-6-22.
 */
object testData {
  val conf = new SparkConf().setAppName("MyTest").setMaster("local[2]").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)

  def toDouble(s:String) ={
    if("?".equals(s))Double.NaN else s.toDouble //判断s中是否存在非法字符，有的话使用Double.NaN
  }

  def parse1(line:String) ={
    val pieces = line.split(",")
    val label = toDouble(pieces(0))
    val scores = pieces(1).split(" ").slice(0,3).map(toDouble)  //slice选取0到第3列，正常用所有的可以省略,转换方法用自定义toDouble
    (label,scores)
    //这种方式访问需要使用 ._1, ._2
  }

  case class MatchData(label:Double,scores:Array[Double])
  def parse2(line:String) ={
    val (label_ori,scores_ori) = line.span(_ != '\t') //span,在第一个满足条件的地方split
    val label = label_ori.toDouble
    val scores = scores_ori.split(" ").slice(0,3).map(toDouble)  //slice选取0到第3列，正常用所有的可以省略,转换方法用自定义toDouble
    MatchData(label,scores)
    //这种方式可以通过 .label, .scores来访问
  }

  def main(args: Array[String]) {
    val data = sc.textFile("test/src/main/resources/test.txt")
    val parsed_data1 = data.map(parse1) //把line处理单独放到parse中
    val parsed_data2 = data.map(parse2)

    val sort_data = parsed_data1.sortBy(_._2(0),false)  //sortBy指定按照哪个排序
    sort_data.foreach(println)

    import java.lang.Double.isNaN
    parsed_data1.map(md=>md._2(0)).filter(!isNaN(_)).stats()  //使用java的isNan来判断是否存在NaN,stats输出统计信息，RDD[Double]类型隐式调用

  }
}
