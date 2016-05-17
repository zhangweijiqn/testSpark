package MlLibTest.test003_ClassifyAndRegress

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-2-19.
 *Various related regression methods are derived by using different types of regularization: ordinary least squares or linear least squares uses no regularization;
 * ridge regression uses L2 regularization; and Lasso uses L1 regularization.
 *
 *Train a linear regression model with no regularization using Stochastic Gradient Descent（随机梯度下降法训练线性回归模型）.
 *
 */
object testLinearRegression {

  //构造函数初始化SparkContext
  val conf = new SparkConf().setAppName("MyTest").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)

  def test(): Unit ={
    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.regression.LinearRegressionModel
    import org.apache.spark.mllib.regression.LinearRegressionWithSGD
    import org.apache.spark.mllib.linalg.Vectors

    // Load and parse the data
    val data = sc.textFile("src/main/resources/lpsa.data")    //导入数据格式（label, x y z），多元线性回归
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()

    // Building the model
    val numIterations = 100
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      println(point.label, prediction)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
    println("training Mean Squared Error = " + MSE)

    // Save and load model
    model.save(sc, "target/tmp/myLinearRegressionModel")
    val sameModel = LinearRegressionModel.load(sc, "target/tmp/myLinearRegressionModel")
  }

  def main(args: Array[String]) {
    test
  }

}
