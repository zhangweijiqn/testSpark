package MlLibTest.test004_Cluster

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-4-7.
 * LDA（Latent Dirichlet Allocation）隐含狄利克雷分布，是一种文档主题生成模型
 */
object testLDA {
  val conf = new SparkConf().setAppName("testKMeans").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
     val data = sc.textFile("src/main/resources/sample_lda_data.txt")
     val parsedData = data.map(row=>Vectors.dense(row.trim.split(" ").map(_.toDouble)))

    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()
    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(3).run(corpus)
    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
      println()
    }

    // Save and load model.
    ldaModel.save(sc, "target/tmp/cluster/myLDAModel")
    val sameModel = DistributedLDAModel.load(sc, "target/tmp/cluster/myLDAModel")

  }
}
