package MlTest.test002_pipeline

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zhangwj on 16-6-28.
 */
object testStringIndexer {
  val sc = new SparkContext(new SparkConf().setAppName("testPipeline").setMaster("local[2]"))
  val sqlContext = new SQLContext(sc)
  val sqlCtx = new SQLContext(sc)

  def main(args: Array[String]) {
    val parsedRDD = sc.textFile("test/src/main/resources/test.txt").map(_.split(" ")).map(eachRow => {
      val a = eachRow.map(x => x.toDouble)
      (a(0),a(1),a(2),a(3),a(4),a(5))
    })
    val df = sqlCtx.createDataFrame(parsedRDD).toDF(
      "label","f0","f1","f2","f3","f4").cache()

    /** *
      * Step 2
      * StringIndexer encodes a string column of labels
      * to a column of label indices. The indices are in [0, numLabels),
      * ordered by label frequencies.
      * This can help detect label in raw data and give it an index automatically.
      * So that it can be easily processed by existing spark machine learning algorithms.
      * */

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(df)
    //大概过程是先将列fit进去，StringIndexer会做一个转换，保存一个kv类型的数据，对label进行映射，后面transform将传入的数据label进行转换。
    val labelIndex = labelIndexer.transform(df)
//    labelIndex.foreach(println)

  }
}
