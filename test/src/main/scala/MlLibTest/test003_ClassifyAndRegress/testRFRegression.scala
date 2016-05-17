package MlLibTest.test003_ClassifyAndRegress

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhangwj on 16-2-22.
  */
object testRFRegression {

   //构造函数初始化SparkContext
   val conf = new SparkConf().setAppName("MyTest").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
   val sc = new SparkContext(conf)

   def main(args: Array[String]) {
     val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "src/main/resources/sample_libsvm_data.txt")
     // Split the data into training and test sets (30% held out for testing)
     val splits = data.randomSplit(Array(0.7, 0.3))
     val (trainingData, testData) = (splits(0), splits(1))

     // Train a RandomForest model.
     // Empty categoricalFeaturesInfo indicates all features are continuous.
     val numClasses = 2
     val categoricalFeaturesInfo = Map[Int, Int]()
     val numTrees = 3 // Use more in practice.
     val featureSubsetStrategy = "auto" // Let the algorithm choose.
     val impurity = "variance"
     val maxDepth = 4
     val maxBins = 32

     import org.apache.spark.mllib.tree.RandomForest
     import org.apache.spark.mllib.tree.model.RandomForestModel

     val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,// Empty categoricalFeaturesInfo indicates all features are continuous.
       numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

     // Evaluate model on test instances and compute test error
     val labelsAndPredictions = testData.map { point =>
       val prediction = model.predict(point.features)
       (point.label, prediction)
     }
     val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
     println("Test Mean Squared Error = " + testMSE)
     println("Learned regression forest model:\n" + model.toDebugString)

     // Save and load model
     model.save(sc, "target/tmp/myRandomForestRegressionModel")
     val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestRegressionModel")


   }
 }
