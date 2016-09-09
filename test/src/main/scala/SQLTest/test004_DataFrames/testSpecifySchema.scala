package SQLTest.test004_DataFrames

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-3-2.
 */
object testSpecifySchema {
  def main(args: Array[String]) {

    //context
    val conf = new SparkConf().setAppName("testSQL").setMaster("local[2]").setSparkHome("")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // create a RDD
    val peopleRDD = sc.textFile("src/main/resources/people.txt")

    // the schema is encoded in a  string
    val schemaString = "name age"

    // Import Row.
    import org.apache.spark.sql.Row;
    // import datatypes
    import org.apache.spark.sql.types.{StructType,StructField,StringType};

    // Generate the schema based on the string of schema
    val schema = StructType(schemaString.split(" ").map(fieldName=>StructField(fieldName,StringType,true)))

    //convert records of the RDD (people) to rows
    val rowRDD = peopleRDD.map(_.split(",")).map(r=>Row(r(0),r(1).trim))

    //apply the schema to RDD   ,将创建的schema应用到RDD上
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD,schema)


    //register the dataframe as a  table
    peopleDataFrame.registerTempTable("people")

    val results = sqlContext.sql("select name,age from people where age>=20")
    println("name , age")
//    results.map(r=>"["+r(0)+","+r(1)+"]").foreach(println)

  }
}
