package MlLibTest.test004_Cluster

import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-4-7.
 * 二分k均值，层次聚类
 */
object testBisecting_KMeans {
  val conf = new SparkConf().setAppName("testBisectKMeans").setMaster("local[2]");
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    //load and parse data
    def parse(line:String)=Vectors.dense(line.split(" ").map(_.toDouble))
    val data = sc.textFile("src/main/resources/kmeans_data.txt").map(parse).cache()

    // Clustering the data into 6 clusters by BisectingKMeans.
    val bkm = new BisectingKMeans().setK(2)
    val model = bkm.run(data)

    val predict=data.map(x=>model.predict(x))
    predict.foreach(println)

    // Show the compute cost and the cluster centers
    println(s"Compute Cost: ${model.computeCost(data)}")
    model.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
      println(s"Cluster Center ${idx}: ${center}")
    }
  }

}
