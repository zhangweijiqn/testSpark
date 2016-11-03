package XGBoost

/**
 * Created by zhangwj on 16-7-20.
 *
 *  xgboost4j
 *
 * install: https://github.com/dmlc/xgboost/blob/master/doc/build.md
 * get started: https://github.com/dmlc/xgboost/blob/master/doc/get_started/index.md
 * 在源代码的xgboost/jvm-packages/xgboost4j/target路径下有 xgboost生成的jar包
 *
 *  mvn install:install-file -Dfile=xgboost4j-0.7.jar -DgroupId=ml.dmlc -DartifactId=xgboost4j -Dversion=0.7 -Dpackaging=jar
 *
 *  有专门xgboost on spark
 * */

import ml.dmlc.xgboost4j.java.Booster
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils
import ml.dmlc.xgboost4j.scala.spark.XGBoost

object SparkWithRDD {
  def main(args: Array[String]): Unit = {

    // if you do not want to use KryoSerializer in Spark, you can ignore the related configuration
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("XGBoost-spark-example")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[Booster]))
    val sc = new SparkContext(sparkConf)
    val inputTrainPath = "test/src/main/resources/sample_libsvm_data.txt"
    val outputModelPath = "test/src/main/target/xgboost4j/spark"
    // number of iterations
    val numRound = 100
    val trainRDD = MLUtils.loadLibSVMFile(sc, inputTrainPath)
    // training parameters
    val paramMap = List(
      "eta" -> 0.1f,
      "max_depth" -> 2,
      "nthread"->1,   //没有这个参数会报错
      "objective" -> "binary:logistic").toMap
    // use 5 distributed workers to train the model
    // useExternalMemory indicates whether
    val model = XGBoost.trainWithRDD(trainRDD, paramMap, numRound, nWorkers = 1, useExternalMemory = true)
    // save model to HDFS path
//    model.saveModelAsHadoopFile(outputModelPath)
  }
}

