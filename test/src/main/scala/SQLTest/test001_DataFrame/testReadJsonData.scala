//package SQLTest.test001_DataFrame

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by zhangwj on 16-7-6.
 */
case class Entity1( name:String,url:String,introduct:String,info:Map[String,String])

object testReadJsonData {

  val conf = new SparkConf().setAppName("hello").setMaster("local[2]")
  val sc = new SparkContext(conf)
  //The entry point into all functionality in Spark SQL is the SQLContext class, or one of its descendants. To create a basic SQLContext, all you need is a SparkContext.0
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {

    import sqlContext.implicits._
    //create dataframe
    val df = sqlContext.read.json("test/src/main/resources/data/data_jbk.json").as[Entity1].toDF()
    df.show

    println("console run test!")

  }
}
