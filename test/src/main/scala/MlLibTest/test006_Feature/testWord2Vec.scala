package MlLibTest.test006_Feature

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
/**
 * Created by zhangwj on 16-6-21.
 * word2vec是一个将单词转换成向量形式的工具
 */
object testWord2Vec {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("word2vec").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val input = sc.textFile("test/src/main/resources/sample_fpgrowth.txt").map(line => line.split(" ").toSeq).distinct()

    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val vectors = model.getVectors
    println("size="+vectors.size)
    vectors.foreach{x=>
      println(x._1+":"+x._2.mkString(","))
    }
    println("----------"*10)

    val synonyms = model.findSynonyms("z", 40)
    println(s"synonyms size = "+synonyms.size)
    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym --- $cosineSimilarity")
    }


    // Save and load model
    model.save(sc, "test/target/word2vect/myModelPath")
    val sameModel = Word2VecModel.load(sc, "test/target/word2vect/myModelPath")
  }

}
