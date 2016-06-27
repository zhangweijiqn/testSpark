package MlLibTest.test007_DimensionalityReduction

import org.apache.spark.mllib.linalg
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Matrix, Matrices}

/**
 * Created by zhangwj on 16-4-17.
 *Singular value decomposition (SVD) 奇异值分解
 * SVD : https://en.wikipedia.org/wiki/Singular_value_decomposition
 */
object testSVD {

  val conf = new SparkConf().setAppName("SVDExample").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val rows: RDD[Vector] = sc.parallelize(Seq(Vectors.dense(1,2,3,4),Vectors.dense(5,6,7,8),Vectors.dense(2,3,4,5))) // an RDD of local vectors
    rows.cache()  //计算过程中会多次使用rdd，事先将rdd缓存起来
    // Create a RowMatrix from an RDD[Vector].
    val mat: RowMatrix = new RowMatrix(rows)  //将RDD[Vector]转换为二维的Matrix

    // Compute the top 20 singular values and corresponding singular vectors.
    val svd: linalg.SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(20, computeU = true)
    val U: RowMatrix = svd.U // The U factor is a RowMatrix.
    val s: Vector = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V // The V factor is a local dense matrix.
  }
}
