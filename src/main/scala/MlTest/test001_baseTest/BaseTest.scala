package MlTest.test001_baseTest

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{ SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zhangwj on 16-3-7.
 *
 * https://spark.apache.org/docs/latest/ml-guide.html#overview-estimators-transformers-and-pipelines-sparkml
 *
 * Example: Estimator, Transformer, and Param
 *
 */
object BaseTest {

  val conf = new SparkConf().setAppName("Mltest").setMaster("spark://zhangwj-OptiPlex-3020:7077").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // Prepare training data from a list of (label, features) tuples.
  val training = sqlContext.createDataFrame(
    Seq( (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5)) )
  ).toDF("label","features")          //toDF Returns a new [[DataFrame]] with columns renamed.
  //toDF 有两种重载的方式，这种方式可以给列命名：这样的DataFrame会一起传入model中

  // Create a LogisticRegression instance.  This instance is an Estimator.
  val lr = new LogisticRegression()

  // Prepare test data.
  val test = sqlContext.createDataFrame(Seq(
    (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
    (0.0, Vectors.dense(3.0, 2.0, -0.1)),
    (1.0, Vectors.dense(0.0, 2.2, -1.5))
  )).toDF("label", "features")

  def testModel1(): Unit ={
      //第一种 参数设置的方式：set方法

    // Print out the parameters, documentation, and any default values.
    println("logistic regression parameters:\n"+lr.explainParams()+"\n")

    // We may set parameters using setter methods.
    lr.setMaxIter(10).setRegParam(0.01)

    // Learn a LogisticRegression model.  This uses the parameters stored in lr.
    val model1 = lr.fit(training)   //estimator lr calls fit to train the model

    // Since model1 is a Model (i.e., a Transformer produced by an Estimator),
    // we can view the parameters it used during fit().
    // This prints the parameter (name: value) pairs, where names are unique IDs for this
    // LogisticRegression instance.
    println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)
    // Make predictions on test data using the Transformer.transform() method.
    // LogisticRegression.transform will only use the 'features' column.
    // Note that model2.transform() outputs a 'myProbability' column instead of the usual
    // 'probability' column since we renamed the lr.probabilityCol parameter previously.
    import org.apache.spark.sql.Row
    model1.transform(test)      //transform
      .select("features", "label", "probability", "prediction") //产生的结果中，包含四个列
      .collect()
      .foreach {
                //x=>println(x(0),x(1),x(2),x(3))     // Row(features: Vector, label: Double, prob: Vector, prediction: Double)
        case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>    //这里的vector引用的是mllib中的vector
          println(s"($features, $label) -> prob=$prob, prediction=$prediction")
      }
  }

  def testModel2(): Unit ={
      //第二种 参数设置的方式：使用ParamMap

    // We may alternatively specify parameters using a ParamMap,
    // which supports several methods for specifying parameters.
    val paramMap = ParamMap(lr.maxIter -> 20)
    .put(lr.maxIter, 30) // Specify 1 Param.  This overwrites the original maxIter.
    .put(lr.regParam -> 0.1, lr.threshold -> 0.55) // Specify multiple Params.

    // One can also combine ParamMaps.
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability") // Change output column name
    val paramMapCombined = paramMap ++ paramMap2

    // Now learn a new model using the paramMapCombined parameters.
    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
    val model2 = lr.fit(training, paramMapCombined)
    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)


    // Make predictions on test data using the Transformer.transform() method.
    // LogisticRegression.transform will only use the 'features' column.
    // Note that model2.transform() outputs a 'myProbability' column instead of the usual
    // 'probability' column since we renamed the lr.probabilityCol parameter previously.
    import org.apache.spark.sql.Row
    model2.transform(test)
      .select("features", "label", "myProbability", "prediction")
      .collect()
      .foreach {
//        x=>println(x(0),x(1),x(2),x(3))
//        Row(features: Vector, label: Double, prob: Vector, prediction: Double)
                case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>    //这里的vector引用的是mllib中的vector
                  println(s"($features, $label) -> prob=$prob, prediction=$prediction")
      }
  }

  def main(args: Array[String]) {
    testModel1()
//    testModel2()
  }


}
