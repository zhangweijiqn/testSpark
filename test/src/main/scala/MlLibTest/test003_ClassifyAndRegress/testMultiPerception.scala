package MlLibTest.test003_ClassifyAndRegress

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

/**
 * Created by zhangwj on 16-6-27.
 */
object testMultiPerception {

    val conf = new SparkConf().setAppName("testBayes").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    def main(args: Array[String]): Unit = {

      // $example on$
      // Load the data stored in LIBSVM format as a DataFrame.
      val data = sqlContext.read.format("libsvm").load("test/src/main/resources/sample_multiclass_classification_data.txt")
      // Split the data into train and test
      val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)
      val train = splits(0)
      val test = splits(1)
      // specify layers for the neural network:
      // input layer of size 4 (features), two intermediate of size 5 and 4
      // and output of size 3 (classes)
      val layers = Array[Int](4, 5, 4, 3)
      // create the trainer and set its parameters
      val trainer = new MultilayerPerceptronClassifier()
        .setLayers(layers)
        .setBlockSize(128)
        .setSeed(1234L)
        .setMaxIter(100)
      // train the model
      val model = trainer.fit(train)
      // compute accuracy on the test set
      val result = model.transform(test)
      val predictionAndLabels = result.select("prediction", "label")
      predictionAndLabels.show

      val evaluator = new MulticlassClassificationEvaluator()
        .setMetricName("f1")
      println("f1: " + evaluator.evaluate(predictionAndLabels))
      // $example off$

    }
}
