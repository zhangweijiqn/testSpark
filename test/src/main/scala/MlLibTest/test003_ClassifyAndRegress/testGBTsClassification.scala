package MlLibTest.test003_ClassifyAndRegress

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
/**
 * Created by zhangwj on 16-2-22.
 */
object testGBTsClassification {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DecisionTreeClassificationExample")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    // $example on$
    // Load and parse the data file.格式LabeledPoint
    val data = MLUtils.loadLibSVMFile(sc, "src/main/resources/sample_libsvm_data.txt")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 5
    //boostingStrategy.setNumIterations(3) // Note: Use more iterations in practice.文档中的 boostingStrategy.numIterations = 3 有问题
    //boostingStrategy.treeStrategy.setMaxDepth(5)
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
/*
*
* Usage tips

We include a few guidelines for using GBTs by discussing the various parameters. We omit some decision tree parameters since those are covered in the decision tree guide.

    loss: See the section above for information on losses and their applicability to tasks (classification vs. regression). Different losses can give significantly different results, depending on the dataset.

    numIterations: This sets the number of trees in the ensemble. Each iteration produces one tree. Increasing this number makes the model more expressive, improving training data accuracy. However, test-time accuracy may suffer if this is too large.

    learningRate: This parameter should not need to be tuned. If the algorithm behavior seems unstable, decreasing this value may improve stability.

    algo: The algorithm or task (classification vs. regression) is set using the tree [Strategy] parameter.

* */
    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)
    println(model.toDebugString)//可以看到每棵树


    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification GBT model:\n" + model.toDebugString)

    // Save and load model
    model.save(sc, "target/tmp/myGradientBoostingClassificationModel")
    val sameModel = GradientBoostedTreesModel.load(sc,
      "target/tmp/myGradientBoostingClassificationModel")


  }
}
