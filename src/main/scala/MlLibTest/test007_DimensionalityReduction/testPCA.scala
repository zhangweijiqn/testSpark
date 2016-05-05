package MlLibTest.test007_DimensionalityReduction

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-4-17.
 *
 * PCA: https://en.wikipedia.org/wiki/Principal_component_analysis
 */
object testPCA {
  val conf = new SparkConf().setAppName("PCAExample").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

  }

  def testRowMatrixPCA(): Unit ={
    import org.apache.spark.mllib.linalg.Matrix
    import org.apache.spark.mllib.linalg.distributed.RowMatrix

    val rows: RDD[Vector] = sc.parallelize(Seq(Vectors.dense(1,2,3,4),Vectors.dense(5,6,7,8),Vectors.dense(2,3,4,5))) // an RDD of local vectors
    // Create a RowMatrix from an RDD[Vector].
    val mat: RowMatrix = new RowMatrix(rows)  //将RDD[Vector]转换为二维的Matrix

    // Compute the top 10 principal components.
    val pc: Matrix = mat.computePrincipalComponents(3) // Principal components are stored in a local dense matrix.

    // Project the rows to the linear space spanned by the top 10 principal components.
    val projected: RowMatrix = mat.multiply(pc)
  }

  def testLabeledPointPCA(): Unit ={
    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.feature.PCA

    val data: RDD[LabeledPoint] =MLUtils.loadLibSVMFile(sc, "src/main/resources/sample_libsvm_data.txt")

    // Compute the top 10 principal components.
    val pca = new PCA(10).fit(data.map(_.features))

    // Project vectors to the linear space spanned by the top 10 principal components, keeping the label
    val projected = data.map(p => p.copy(features = pca.transform(p.features)))
  }
}
