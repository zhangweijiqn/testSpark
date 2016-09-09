package MlLibTest.test001_Data

import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

/**
 * Created by zhangwj on 16-9-8.
 */
object testSparkCsv {

  val conf = new SparkConf().setAppName("testSparkCsv").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def testCars(): Unit ={
    val customSchema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true),
      StructField("comment", StringType, true),
      StructField("blank", StringType, true)))

    val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "test/src/main/resources/cars.csv", "header" -> "true"))
    val selectedData = df.select("year", "model")
    selectedData.rdd.saveAsTextFile("test/target/newcars.csv")
  }

  def testTitanic(): Unit ={
    //train_titanic.csv的数据name字段是被双引号引起来的，里面包含逗号，spark-csv可以处理这种情况
    val customSchema = StructType(Array(
      StructField("PassengerId", IntegerType, true),
      StructField("Survived", IntegerType, true),
      StructField("Pclass", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Sex", StringType, true),
      StructField("Age", DoubleType, true),
      StructField("SibSp", IntegerType, true),
      StructField("Parch", IntegerType, true),
      StructField("Ticket", StringType, true),
      StructField("Fare", DoubleType, true),
      StructField("Cabin", StringType, true),
      StructField("Embarked", StringType, true)))

    val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "test/src/main/resources/train_titanic.csv", "header" -> "true"))
    val selectedData = df.select("Survived", "Pclass", "Sex", "Age", "SibSp", "Parch", "Fare", "Embarked")
    selectedData.rdd.saveAsTextFile("test/target/new_train_titanic.csv")
    val nameField = df.select("Name")
    nameField.collect().foreach(println)
  }

  def main(args: Array[String]) {
    testCars
    testTitanic
  }
}
