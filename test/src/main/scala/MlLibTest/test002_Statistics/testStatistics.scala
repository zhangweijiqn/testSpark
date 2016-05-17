package MlLibTest.test002_Statistics

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.mllib.stat.{Statistics, MultivariateStatisticalSummary}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-2-17.
 * https://spark.apache.org/docs/latest/mllib-statistics.html
 */
object testStatistics {

  import org.apache.spark.mllib.linalg.{Vectors, Vector}  //需要引入mllib的vector，否则会使用scala vector，编译不通过
  var observations: RDD[Vector]=null
  val conf = new SparkConf().setAppName("MyTest").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)

  def getData(): Unit ={
    //加载数据

    val data = sc.textFile("src/main/resources/test.txt").map(_.split(" ")).map(_.map(_.toDouble))
    data.foreach(_.foreach(println _))
    observations = data.map(x => Vectors.dense(x))  //要转换为RDD[Vector]类型,一个vector代表一行
  }

  def testBasic(): Unit ={

    // Compute column summary statistics.
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)   //在Statistics
    println(summary.mean) // a dense vector containing the mean value for each column，计算每个列的均值
    println(summary.variance) // column-wise variance
    println(summary.numNonzeros) // number of nonzeros in each column
  }

  def testCorrelations(): Unit ={

    val seriesX: RDD[Double] =  sc.parallelize(List(1.0,3.3,2.1,4.5,5.6))// a series
    val seriesY: RDD[Double] = sc.makeRDD(List(2.0,4.3,3.1,5.5,6.6)) // must have the same number of partitions and cardinality as seriesX
    val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")  //  ??? 得到的值为NaN
    println("correlation = " + correlation)

    import org.apache.spark.mllib.linalg.Matrix
    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
    // If a method is not specified, Pearson's method will be used by default.
    val correlMatrix: Matrix = Statistics.corr(observations, "pearson") //相关矩阵第i行第j列的元素是原矩阵第i列和第j列的相关系数。

    println(correlMatrix)
  }

  def testHypothesisTesting(): Unit ={
    //卡方检验，读取数据格式为：sparse data，计算每个feature的检验值（即每列的卡方检验）
    // The contingency table is constructed from the raw (feature, label) pairs and used to conduct
    // the independence test. Returns an array containing the ChiSquaredTestResult for every feature
    // against the label.
    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "src/main/resources/sample_libsvm_data.txt")
    val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(examples)
    var i = 1
    featureTestResults.foreach { result =>
      println(s"Column $i:\n$result")
      i += 1
    } // summary of the test
  }

  def testStratifiedSampling(): Unit ={
    //分层抽样
  }

  def testRandomDataGeneration(): Unit ={
    import org.apache.spark.mllib.random.RandomRDDs._
    // Generate a random double RDD that contains 1 million i.i.d. values drawn from the
    // standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
    val u = normalRDD(sc, 10L, 10)
    u.foreach(x => print(x+" "))  //产生了10个服从N(0,1)的正态分布
    // Apply a transform to get a random double RDD following `N(1, 4)`.
    val v = u.map(x => 1.0 + 2.0 * x)
    v.foreach(x => print(x+" "))  ////产生了10个服从N(1,2)的正态分布
  }

  def testKernelDensityEstimation(): Unit ={
    import org.apache.spark.mllib.stat.KernelDensity
    import org.apache.spark.rdd.RDD
    //核密度估计
    // Construct the density estimator with the sample data and a standard deviation for the Gaussian kernels
    val data = sc.parallelize(List(1.0,2.0,3.0,3.0,3.0,4.0,4.0,4.0,5.0,6.0,7.0,8.0))
    val kd = new KernelDensity()
      .setSample(data)
      .setBandwidth(0.5)  //bandwith用来进行smoothing的自由参数，太小会undersmooth,太大会oversmooth,

    // Find density estimates for the given values
    val densities = kd.estimate(Array(1.0, 2.0, 3.0,4.0,5.0,6.0,7.0,8.0))   //得到的估计值可以画出核密度估计曲线，可以发现3.0,4.0的值比较大
    densities.foreach(println _)
  }

  def main(args: Array[String]) {
//    getData()
//    testBasic
//    testCorrelations
//    testHypothesisTesting
//    testRandomDataGeneration
    testKernelDensityEstimation
  }
}
