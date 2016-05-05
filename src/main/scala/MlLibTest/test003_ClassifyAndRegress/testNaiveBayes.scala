package MlLibTest.test003_ClassifyAndRegress

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-3-21.
 */
object testNaiveBayes {
  val conf = new SparkConf().setAppName("testBayes").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val data = sc.textFile("src/main/resources/sample_naive_bayes_data.txt")
    val parsedData = data.map{ line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }
    // Split data into training (60%) and test (40%).
    val splits = parsedData.randomSplit(Array(0.6,0.4),seed=11L)
    val training = splits(0)
    val test = splits(1)

    import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
    val model = NaiveBayes.train(training,lambda = 1.0, modelType = "multinomial")

    // predict label
    val predictionAndLabel = test.map( p=> (model.predict(p.features),p.label)) //对test中的每个样本逐一预测，样本可以通过 p.features 取得特征值,通过 p.label取得类标
    predictionAndLabel.foreach(println)
    val accuracy = 1.0*predictionAndLabel.filter(x => x._1==x._2).count()/test.count(); //求准确率，使用 filter 方法来过滤预测不正确的
    println("accuracy="+accuracy)

    // predict probability
    val probabilityAndLabel = test.map( p=> (model.predictProbabilities(p.features),p.label)) //对test中的每个样本逐一预测，样本可以通过 p.features 取得特征值,通过 p.label取得类标
    probabilityAndLabel.foreach(println)  //输出样本属于每个类的概率

  }

}
