package SQLTest.test001_DataFrame

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 *  Created by zhangwj on 16-3-1.
 *  https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame
 */
object testDataFrame {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("hello").setMaster("local[2]").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)

    //The entry point into all functionality in Spark SQL is the SQLContext class, or one of its descendants. To create a basic SQLContext, all you need is a SparkContext.0
    val sqlContext = new SQLContext(sc)

    //create dataframe
    val df = sqlContext.read.json("src/main/resources/people.json")

    //DataFrame Operations

    // Displays the content of the DataFrame to stdout
    df.show()

    // Print the schema in a tree format
    df.printSchema()

    // Select only the "name" column
    df.select("name").show()

    // Select everybody, but increment the age by 1
    df.select(df("name"), df("age") + 1).show()

    // Select people older than 21
    df.filter(df("age") > 21).show()

    // Count people by age
    df.groupBy("age").count().show()

    val tableRDD = df.rdd.map(x=>(x(0),x.getString(1)))
    tableRDD.foreach(println)

  }
}
