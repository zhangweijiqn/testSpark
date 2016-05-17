package MlTest.test003_ModelSelection

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zhangwj on 16-3-7.
 * Example: model selection via cross-validation (交叉验证是闭测参数寻优的时候）
 *
 * An important task in ML is model selection, or using data to find the best model or parameters for a given task.
 */
object testModelSelection {

  val sc = new SparkContext(new SparkConf().setAppName("testPipeline").setMaster("local[2]"))
  val sqlContext = new SQLContext(sc)

  def testCrossValidation(): Unit ={
    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
    import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
    import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
    import org.apache.spark.mllib.linalg.Vector
    import org.apache.spark.sql.Row

    // Prepare training data from a list of (id, text, label) tuples.
    val training = sqlContext.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0),
      (4L, "b spark who", 1.0),
      (5L, "g d a y", 0.0),
      (6L, "spark fly", 1.0),
      (7L, "was mapreduce", 0.0),
      (8L, "e spark program", 1.0),
      (9L, "a e c l", 0.0),
      (10L, "spark compile", 1.0),
      (11L, "hadoop software", 0.0)
    )).toDF("id","text","label")

    // Configure an ML pipeline, which consists of three stages: tokenizer(分词), hashingTF（统计词频）, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
    val pipeline = new Pipeline().setStages(Array(tokenizer,hashingTF,lr))    //将三个状态传递给Pipeline,此时pipeline是一个estimator

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000)) // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
      .addGrid(lr.regParam, Array(0.1, 0.01)) // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2) // Use 3+ in practice
    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(training)

    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test = sqlContext.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    cvModel.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
  }

  def testCrossValidation2(): Unit ={
    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
    import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
    import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

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

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3) // Use 3+ in practice
    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(train)   //train集合内部采用交叉验证，外部使用的train:test = 0.9:0.1

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    cvModel.transform(test)
      .select("features", "label", "prediction")
      .show()
  }

  def main(args: Array[String]) {
    testCrossValidation2()
  }
}
