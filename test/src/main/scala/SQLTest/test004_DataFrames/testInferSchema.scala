package SQLTest.test004_DataFrames

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-3-2.
 * 读取数据到表中，从 case class 推断 schema
 */
object testInferSchema {

  // Define the schema using a case class.
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,（case class最大支持22fields）
  // you can use custom classes that implement the Product interface.（更大的fields定义普通类实现Product接口）
  case class Person(name:String,age:Int)    //注意 case class 放在 main 外面，否则会报 No TypeTag available for Person 的错误

  def main(args: Array[String]) {
    //context
    val conf = new SparkConf().setAppName("testSQL").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    //Create an RDD of Person objects, 从文件中读取字段值绑定到 case class, 得到的RDD转化为 DataFrame
    val peopleDF = sc.textFile("src/main/resources/people.txt").map(_.split(",")).map(p=>Person(p(0),p(1).trim.toInt)).toDF()
    //and register it as a table.
    peopleDF.registerTempTable("people")  //把DataFrame注册为数据表

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("select name,age from people where age>=13 and age<=19")   //teenagers是DataFrame类型，可以执行RDD的所有操作
    println(teenagers)

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index:
    teenagers.map(r=>"name:"+r(0)).collect().foreach(println)   //   对每行的字段使用 索引 就可以取值：　r(index)

    //get field by name,按名称得到字段
    teenagers.map(r=>"name:"+r.getAs[String]("name")).collect().foreach(println)

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T],检索多列  getValuesMap
    teenagers.map( _.getValuesMap[Any](List("name","age")) ).collect().foreach(println) //each row is:  Map(name -> Michael1, age -> 18)
  }
}
