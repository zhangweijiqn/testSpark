package SQLTest.test002_DataSets

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhangwj on 16-3-1.
 *  https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset
  */
object testDataSets {
   def main(args: Array[String]) {

     val conf = new SparkConf().setAppName("testSQL").setMaster("local[2]").setSparkHome("")
     val sc = new SparkContext(conf)

     val sqlContext = new SQLContext(sc)

     import sqlContext.implicits._
     // Encoders for most common types are automatically provided by importing sqlContext.implicits._

     val ds = Seq(1,2,3).toDS
     ds.map(_+1).collect()  //return Array(2,3,4)

     case class Person(name: String, age: Long) //must be case class
//     val ds2 = Seq(Person("Andy",32L)).toDS()

     // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
     val path = "src/main/resources/people.json"
//     val people = sqlContext.read.json(path).as[Person]


   }
 }
