package MlLibTest.test003_ClassifyAndRegress

/**
 * Created by zhangwj on 16-2-15.
 * https://spark.apache.org/docs/latest/mllib-decision-tree.html
 */

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object testDecisionTree {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("DecisionTreeClassificationExample")
    .setSparkHome(System.getenv("SPARK_HOME"))
    .setMaster("local")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data file.格式LabeledPoint
    val data = MLUtils.loadLibSVMFile(sc, "hdfs://localhost:9000/user/hadoop/mllib/sample_libsvm_data.txt") //read hdfs files
//    val data = MLUtils.loadLibSVMFile(sc, "src/main/resources/sample_libsvm_data.txt")  //read local file
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)  //决策树模型输出不了概率
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)

    // Save and load model
    model.save(sc, "./target/tmp/myDecisionTreeClassificationModel")
    val sameModel = DecisionTreeModel.load(sc, "./target/tmp/myDecisionTreeClassificationModel")
    // $example off$
  }
}
