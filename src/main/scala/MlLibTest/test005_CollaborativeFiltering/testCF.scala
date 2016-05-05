package MlLibTest.test005_CollaborativeFiltering

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer


/**
 * Created by zhangwj on 16-4-12.
 */
object testCF {

  val conf = new SparkConf().setMaster("local[2]").setAppName("testCF")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc) //可以替换为hiveContext
  case class Employees(user_log_acct:String,item_sku_id:String,orderCount:Integer,sale_qtty:Integer,pay_amount:Double)

  def main(args: Array[String]) {

    // Load and parse the data, data format user * features(可以是product也可以是portraits属性)
    val data = MLUtils.loadLibSVMFile(sc,"src/main/resources/sample_libsvm_data.txt")
    val rows: RDD[Vector] = data.map(_.features) // an RDD of local vectors
    // Create a RowMatrix from an RDD[Vector].
    val mat: RowMatrix = new RowMatrix(rows)  //将RDD[Vector]转换为二维的Matrix

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()
    println("m="+m)
    println("n="+n)




  }

  def testLoadData1(): Unit ={
    // Load and parse the data, data format user * features(可以是product也可以是portraits属性)
    val data = sc.textFile("src/main/resources/data/user_product_map.txt").map(_.split("\t"))
    val user_items = data.map(line=>(line(0),line(1),if(!line(2).equals("NULL"))line(2).toInt else 0,if(!line(3).equals("NULL"))line(3).toInt else 0,if(!line(4).equals("NULL"))line(4).toDouble else 0.0))
    //存在 han120	NULL	1	NULL	NULL 这样的数据
    import sqlContext.implicits._
    user_items.toDF.registerTempTable("employees")
    val emplyee_Number = sqlContext.sql("select count(distinct _1) from employees").collect()
    println(emplyee_Number(0).toString().toInt)

    val sku_Number = sqlContext.sql("select count(distinct _2) from employees").collect()
    println(sku_Number(0).toString().toInt)

  }

  def getMatrix(ColumnsNum:Int,RowsNum:Int ):RowMatrix={
    import org.apache.spark.mllib.linalg.distributed.RowMatrix
    import org.apache.spark.mllib.linalg.Vectors
    val row = Vectors.sparse(ColumnsNum,new Array[Int](0),new Array[Double](0))
    val mtrx:ArrayBuffer[Vector] = ArrayBuffer(row)
    for( i <- 1 until RowsNum){
      mtrx+=row
    }

    val rows: RDD[Vector] = sc.parallelize(mtrx) // an RDD of local vectors
    // Create a RowMatrix from an RDD[Vector].
    val mat: RowMatrix = new RowMatrix(rows)  //将RDD[Vector]转换为二维的Matrix

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()
    println("m="+m)
    println("n="+n)
    mat
  }
}
