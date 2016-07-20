package MlTest.test005_Feature

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zhangwj on 16-7-17.
 * word2vec通过将输入的语料库进行训练，将单词/句子转换为向量，可以用来找同义词，模型可以输出每个词及其对应的浮点向量
 * 训练语料库：wget http://mattmahoney.net/dc/text8.zip 解压后大小96M
 */

object testWord2Vec {
  val sc = new SparkContext(new SparkConf().setAppName("testPipeline").setMaster("local[2]"))
  val sqlContext = new SQLContext(sc)
  def main(args: Array[String]) {
    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = sqlContext.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    //val documentDF = sc.textFile("/tmp/data/text8").map(line => line.split(" ").toSeq).distinct().map(Tuple1.apply).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)

    val model = word2Vec.fit(documentDF)


    val synonyms = model.findSynonyms("wish", 3)  //找wish的同义词，3个
    println(synonyms.show)


    val result = model.transform(documentDF)  // Transform a sentence column to a vector column to represent the whole sentence. The transform  * is performed by averaging all word vectors it contains.
    /*
    * +--------------------+--------------------+
      |                text|              result|
      +--------------------+--------------------+
      |[Hi, I, heard, ab...|[-0.0228339155670...|
      |[I, wish, Java, c...|[-0.0323234447278...|
      |[Logistic, regres...|[-0.0220251377671...|
      +--------------------+--------------------+
    * */
    result.select("result").take(3).foreach(println)


  }
}
