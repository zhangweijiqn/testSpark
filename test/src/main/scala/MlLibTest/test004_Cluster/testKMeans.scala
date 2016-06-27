package MlLibTest.test004_Cluster

import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zhangwj on 16-4-6.
 * The spark.mllib implementation includes a parallelized variant of the k-means++ method called kmeans||：https://en.wikipedia.org/wiki/K-means%2B%2B
 */
object testKMeans {
  val conf = new SparkConf().setAppName("testKMeans").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val data_orgn = sc.textFile("src/main/resources/kmeans_data.txt")
    val parsedData = data_orgn.map(s=>Vectors.dense(s.split(" ").map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations) //传入数据行的格式是Vector
    //计算两个点的距离使用的方法是Vectors.sqdist(squared distance between two Vectors.)，目前只支持欧氏距离

    val clusterCenter = clusters.clusterCenters //得到聚类中心
    clusterCenter.foreach(x=>{
      println
      x.foreachActive((index,value)=>print("("+index+","+value+")\t"))
    })

    //输出训练数据的聚类结果
    val predict = parsedData.map(x=>clusters.predict(x))
    predict.foreach(println)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    //save model and load model
    clusters.save(sc,"target/tmp/cluster/kmeans")
    val sameModel = KMeansModel.load("target/tmp/cluster/kmeans")

  }
}
