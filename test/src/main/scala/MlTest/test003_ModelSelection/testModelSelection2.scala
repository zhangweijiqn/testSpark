package MlTest.test003_ModelSelection

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zhangwj on 16-3-7.
 *
 *  model selection via train validation split
 *
 */
object testModelSelection2 {

  val sc = new SparkContext(new SparkConf().setAppName("testPipeline").setMaster("local[2]"))
  val sqlContext = new SQLContext(sc)

  def testSplitValidation(): Unit ={
    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
    import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
    import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
    import org.apache.spark.mllib.linalg.Vector
    import org.apache.spark.sql.Row

    // Prepare training and test data.
    val data = sqlContext.read.format("libsvm").load("src/main/resources/sample_libsvm_data.txt")
    val Array(train,test) = data.randomSplit(Array(0.9,0.1),seed=12345)

    val lr = new LogisticRegression()
    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using the evaluator.
    import org.apache.spark.ml.tuning.ParamGridBuilder
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()


    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val trainValidationSplit = new TrainValidationSplit()
    .setEstimator(lr)
    .setEvaluator(new RegressionEvaluator)
    .setEstimatorParamMaps(paramGrid)
    // 80% of the data will be used for training and the remaining 20% for validation.
    .setTrainRatio(0.8)

    // Run train validation split, and choose the best set of parameters.
    val model = trainValidationSplit.fit(train)

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    model.transform(test)
      .select("features", "label", "prediction")
      .show()

  }

  def main(args: Array[String]) {
    testSplitValidation()
  }
}
