package MlLibTest.test003_ClassifyAndRegress

import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-3-1.
 */
object testGBTsRegression {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("testGBTsRegression").setMaster("local[2]").setSparkHome("/hoem/zhangwj/Applications/spark-1.6.0-bin-hadoop2.6")
    val sc = new SparkContext(conf)

    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc,"src/main/resources/sample_libsvm_data.txt")

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7,0.3))
    val (trainingData,testData) = (splits(0),splits(1))


    // Train a GradientBoostedTrees model.
    // The defaultParams for Regression use SquaredError by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.numIterations = 3
    boostingStrategy.treeStrategy.maxDepth = 5

    //// Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int,Int]()

    val model = GradientBoostedTrees.train(trainingData,boostingStrategy)

    //Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map{
      point =>
        val prediciton  = model.predict(point.features)
        (point.label,prediciton)
    }

    val testMSE = labelsAndPredictions.map{case (v,p)=>math.pow((v-p),2)}.mean()
    println("Test Mean Squared Error="+testMSE)
    println("Learn regressino GBT model:\n"+model.toDebugString)

    //save and load model
    model.save(sc,"target/tmp/myGradientBoostingRegressionModel")

    import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
    val sameModel = GradientBoostedTreesModel.load(sc,"target/tmp/myGradientBoostingRegressionModel");
  }
}
