package MlLibTest.test003_ClassifyAndRegress

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-2-22.
 * The example below demonstrates how to load a LIBSVM data file, parse it as an RDD of LabeledPoint and then perform regression using a decision tree with variance as an impurity measure and a maximum tree depth of 5. The Mean Squared Error (MSE) is computed at the end to evaluate goodness of fit.
 */
object testDTRegression {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DecisionTreeClassificationExample")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    // $example on$
    // Load and parse the data file.格式LabeledPoint
    val data = MLUtils.loadLibSVMFile(sc, "src/main/resources/sample_libsvm_data.txt")
//    val data = MLUtils.loadLibSVMFile(sc, "hdfs://localhost:9000/user/hadoop/mllib/sample_libsvm_data.txt") //read hdfs files
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "variance"
    val maxDepth = 5
    val maxBins = 32

    import org.apache.spark.mllib.tree.DecisionTree
    import org.apache.spark.mllib.tree.model.DecisionTreeModel

    val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity,
      maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testMSE = labelsAndPredictions.map{ case (v, p) => math.pow(v - p, 2) }.mean()
    println("Test Mean Squared Error = " + testMSE)
    println("Learned regression tree model:\n" + model.toDebugString)

    // Save and load model
    model.save(sc, "target/tmp/myDecisionTreeRegressionModel")
    val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeRegressionModel")
  }
}
