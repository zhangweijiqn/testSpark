package SQLTest.test005_dataSource

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-3-3.
 * 可以从普通文本文件json.txt等读取到RDD中，然后转换为DataFrame，最终可以保存为parquet文件里；
 * 也可以从parquet文件中直接读取到DataFrame中
 */
object testDataSource {

  case class Person(name:String,age:Int)    //注意 case class 放在 main 外面，否则会报 No TypeTag available for Person 的错误

  val conf = new SparkConf().setAppName("testDataSource").setMaster("local[2]").setSparkHome("")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def testRW(): Unit ={
    //load json file and
    val df_json = sqlContext.read.format("json").load("src/main/resources/people.json")    //.read.format()来指定读取文件的格式
    //save as parquet file
    df_json.select("name","age").write.format("parquet").save("src/main/resources/people.parquet")   //write.format()来指定保存文件的格式


    // load parquet file
    val df_parquet = sqlContext.read.load("src/main/resources/people.parquet")
    println(df_parquet.select("name","age").count())
    df_parquet.write.format("parquet").mode("overwrite").save("target/tmp/sql/people.parquet")    //可以设置saveMode:error(default),append,overwrite,ignore


    // directly run sql on files
    sqlContext.sql("select * from parquet.`src/main/resources/people.parquet`").collect().foreach(println)
  }

  def testParquet(): Unit = {
    // sqlContext from the previous example is used in this example.
    // This is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    val people =sc.textFile("src/main/resources/people.txt").map(_.split(",")).map(p=>Person(p(0),p(1).trim.toInt)).toDF
    //implicits是一个object ( object implicits extends SQLImplicits ), SQLImplicits 是一个abstract class，
    // SQLImplicits类中提供了一个方法：implicit def rddToDataFrameHolder[A <: Product : TypeTag](rdd: RDD[A]): DataFrameHolder，用于将RDD转换为DataFrameHolder对象
    // DataFrameHolder中提供了 toDF 方法可以返回 DataFrames对象

    // The RDD is implicitly converted to a DataFrame by implicits, allowing it to be stored using Parquet.
    people.write.mode("overwrite").parquet("src/main/resources/people.parquet")


    // Read in the parquet file created above. Parquet files are self-describing so the schema is preserved. parquet文件中保存了schema
    // The result of loading a Parquet file is also a DataFrame.
    val parquetFile = sqlContext.read.parquet("src/main/resources/people.parquet")  //得到的仍然是一个DataFrame


    //Parquet files can also be registered as tables and then used in SQL statements.
    parquetFile.registerTempTable("parquetFile")

    //注册为temp table后可以直接使用sql查询
    val teenagers = sqlContext.sql("select name from parquetFile where age>=18 and age <=25")  //查询的结果类型仍然是DataFrame
    teenagers.map( p=> "name:"+p(0)).collect().foreach(println)

  }

  def main(args: Array[String]) {


    //testRW()

    testParquet()

  }
}
