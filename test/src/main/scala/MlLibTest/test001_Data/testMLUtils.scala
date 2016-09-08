package MlLibTest.test001_Data

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-2-19.
 */
object testMLUtils {

  //构造函数初始化SparkContext
  val conf = new SparkConf().setAppName("MyTest").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)

  def testLoadLabeledData(): Unit ={

    //读取矩阵，格式为（Label,f1 f2 f3...）注意分隔符才（从loadLabeledData可以看出）
    val data = MLUtils.loadLabeledPoints(sc,"src/main/resources/test.txt")
    data.foreach(println _)

    //保存的格式为(Label,[f1,f2,f3,...]
    data.saveAsTextFile("target/resources/saveObejctData.txt")

    //保存为libsvm需要的Sparse Data格式：1.0 1:1.2 2:4.3 3:1.5 4:1.7 5:2.1
    MLUtils.saveAsLibSVMFile(data,"target/resources/savedData.txt")
    //保存的结果中是个文件夹，在文件夹下part-00000就是需要保存的文件
  }

  def test_kFold_Data(): Unit ={
    //kfold交叉验证Data
    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "target/resources/sample_libsvm_data.txt")
    val kFold_data = MLUtils.kFold(data,10,12345)   //10 fold, seed = 12345，返回类型Array[(RDD[T], RDD[T])]，第一个是training data，第二个是validation data
    kFold_data.foreach{   //   1/k data
      case (train,validation)=>{
        for(t<-train){  //t是一个带有label的sample,格式为 sparse vector (1.0,(692,[x,x,x,x,x]))
          println(t)
        }
        println("| , |")
        for(v<-validation)println(v+" ")
        println()
      }
    }
  }

  def main(args: Array[String]) {
//    test_kFold_Data
    testLoadLabeledData()
  }
}
