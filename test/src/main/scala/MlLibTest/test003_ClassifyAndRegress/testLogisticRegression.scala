package MlLibTest.test003_ClassifyAndRegress

/**
 * Created by zhangwj on 16-2-19.
 *
 * 逻辑回归参考文章：https://en.wikipedia.org/wiki/Logistic_regression
 * 可以解决多分类问题
 * 参数寻优有两种实现方式：
 * SGD: https://en.wikipedia.org/wiki/Limited-memory_BFGS
 * L-BFGS: https://en.wikipedia.org/wiki/Limited-memory_BFGS
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
object testLogisticRegression {

  //构造函数初始化SparkContext
  val conf = new SparkConf().setAppName("MyTest").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)

  def test(): Unit ={
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "src/main/resources/sample_libsvm_data.txt")
    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
//      new LogisticRegressionWithSGD
      .setNumClasses(2)
      .run(training)

    model

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      println(prediction, label)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)

    // Save and load model
    model.save(sc, "target/tmp/myLogisticRegressionModel")
    val sameModel = LogisticRegressionModel.load(sc, "target/tmp/myLogisticRegressionModel")

  }

  def main(args: Array[String]) {
    test
  }
}
