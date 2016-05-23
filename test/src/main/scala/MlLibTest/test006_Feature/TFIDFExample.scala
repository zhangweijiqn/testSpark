/*
   reference: https://en.wikipedia.org/wiki/Tf–idf

 * IDF(t,D)=log( |D|+1/(DF(t,D)+1) ),
 * TFIDF(t,d,D)=TF(t,d)⋅IDF(t,D).
 *
 * Note: spark.mllib doesn’t provide tools for text segmentation. We refer users to the Stanford NLP Group and scalanlp/chalk.
 */

// scalastyle:off println
package MlLibTest.test006_Feature

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
// $example off$

object TFIDFExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TFIDFExample").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // $example on$
    // Load documents (one per line).要求每行作为一个document（一篇文章一行）,这里zipWithIndex将每一行的行号作为doc id
    val documents: RDD[Seq[String]] = sc.textFile("src/main/resources/kmeans_data.txt")
      .map(_.split(" ").toSeq)

    val hashingTF = new HashingTF(Math.pow(2, 18).toInt) //hash buckets大小
    val tf: RDD[Vector] = hashingTF.transform(documents)
    //该方法会将词特征映射到一个很大维度的向量中去，每篇文章一个向量，向量长度为numFeatures，这是HashingTF类的成员变量，默认为2的20次方。

    // While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
    // First to compute the IDF vector and second to scale the term frequencies by IDF.
    tf.cache()
    val idf = new IDF().fit(tf) //返回一个 IDFModel 对象

    val tfidf: RDD[Vector] = idf.transform(tf)

    // spark.mllib IDF implementation provides an option for ignoring terms which occur in less than
    // a minimum number of documents. In such cases, the IDF for these terms is set to 0.如果出现次数小于设定的最小值，IDF=0
    // This feature can be used by passing the minDocFreq value to the IDF constructor.
    val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
    val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)
    // $example off$

    println("tfidf: ")
    tfidf.foreach(x => println(x))  //输出是什么???

    println("tfidfIgnore: ")
    tfidfIgnore.foreach(x => println(x))

    sc.stop()
  }
}
// scalastyle:on println
