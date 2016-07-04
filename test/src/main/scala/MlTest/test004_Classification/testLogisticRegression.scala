package MlTest.test004_Classification

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-7-4.
 */
object testLogisticRegression {
  val conf = new SparkConf().setAppName("Mltest").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {
    import org.apache.spark.ml.classification.LogisticRegression

    // Load training data
    val training = sqlContext.read.format("libsvm").load("test/src/main/resources/sample_libsvm_data.txt")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    // 查看LogisitcRegression代码：@org.apache.spark.annotation.Since("1.6.0") val coefficients : org.apache.spark.mllib.linalg.Vector
    // coefficients是1.6才加入的。在1.5的平台上跑会报错。
  }
}
