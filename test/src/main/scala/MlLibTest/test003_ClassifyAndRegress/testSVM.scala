package MlLibTest.test003_ClassifyAndRegress

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Created by zhangwj on 16-2-17.
 * https://spark.apache.org/docs/latest/mllib-linear-methods.html
 *
 * SVM只提供了线性模型，（多项式，径向基，Sigmoid未在文档中找到），并且只能解决二分类问题
 *
 */
object testSVM {

  //构造函数初始化SparkContext
  val conf = new SparkConf().setAppName("MyTest").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    import org.apache.spark.mllib.util.MLUtils

    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "test/src/main/resources/sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100     //numIterations是SVM梯度下降法迭代的次数，还有个参数为stepSize，是梯度下降法每次迭代的step
    val model = SVMWithSGD.train(training, numIterations)   //train重载函数，可以传入多个参数还包括，regParam: Double,miniBatchFraction: Double,initialWeights: Vector)
    //SVM通常需要标准化，训练的时候使用的那种标准化方法？？？
    //The SVMWithSGD.train() method by default performs L2 regularization with the regularization parameter set to 1.0. If we want to configure this algorithm, we can customize SVMWithSGD further by creating a new object directly and calling setter methods.

    /*  import org.apache.spark.mllib.optimization.L1Updater
        val svmAlg = new SVMWithSGD()
        svmAlg.optimizer.
        setNumIterations(200).
        setRegParam(0.1).
        setUpdater(new L1Updater)
        val modelL1 = svmAlg.run(training)
    * */

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features) //point.features取特征值
      (score, point.label)  //point.label取类标
    }
    scoreAndLabels.foreach{
      case (score,label)=> println(s"(${score},${label}})")
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    // Save and load model
    model.save(sc, "target/tmp/mySVMModel")
    val sameModel = SVMModel.load(sc, "target/tmp/mySVMModel")

    //再对测试集进行预测
    val scoreAndLabels2 = test.map { point =>
      val score = sameModel.predict(point.features) //point.features取特征值
      (score, point.label)  //point.label取类标
    }
    scoreAndLabels2.foreach{
      case (score,label)=> println(s"(${score},${label}})")
    }

  }

}
