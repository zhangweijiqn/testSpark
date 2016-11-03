package XGBoost

import ml.dmlc.xgboost4j.java.Booster
import ml.dmlc.xgboost4j.scala.spark.XGBoost
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zhangwj on 16-9-20.
 */
object testTitanicSpark {

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("XGBoost-spark-example")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkConf.registerKryoClasses(Array(classOf[Booster]))
  val sc = new SparkContext(sparkConf)

  def getTrainData(data: RDD[Array[String]], sexFlag: Double, fareFlag: Double): RDD[LabeledPoint] = {
    data.map(x => {
      val survival = x(1)
      val passengerId = x(0)
      val pclass = if (x(2).contentEquals("")) (Math.random() * 3) else x(2).toDouble
      val pclass_log = math.log(pclass + 0.1)
      val sex = if (x(4).contentEquals("male")) 1.0 else if (x(4).contentEquals("female")) 0.0 else sexFlag //取人数最少的性别
      val sex_log = math.log(sex + 0.1)
      val age = if (x(5).isEmpty) (Math.random() * 100) else x(5).toDouble //随机取年龄值
      val age_log = math.log(age + 0.1)
      val sibSp = if (x(6).contentEquals("")) (Math.random() * 8) else x(6).toDouble
      val sibSp_log = math.log(sibSp + 0.1)
      val parch = if (x(7).contentEquals("")) (Math.random() * 6) else x(7).toDouble
      val parch_log = math.log(parch + 0.1)
      //      val ticket = x(8).toDouble
      val fare = if (x(9).contentEquals("")) fareFlag else x(9).toDouble
      val fare_log = math.log(fare + 0.1)
      //      val cabin =
      val embarked = if (x.length <= 11) Math.random() * 2 else if (x(11).contentEquals("C")) 0.0 else if (x(11).contentEquals("S")) 1.0 else 2.0
      val embarked_log = math.log(embarked + 0.1)
      val features: Array[Double] = Array(passengerId.toDouble, pclass.toDouble, pclass_log.toDouble, sex.toDouble, sex_log.toDouble, age.toDouble, age_log.toDouble,
        sibSp.toDouble, sibSp_log.toDouble, parch.toDouble, parch_log.toDouble, fare.toDouble, fare_log.toDouble, embarked.toDouble, embarked_log.toDouble)
      LabeledPoint(survival.toDouble, Vectors.dense(features))
    })
  }

  def getTestData(data: RDD[Array[String]], sexFlag: Double, fareFlag: Double) = {
    data.map(x => {
      val passengerId = x(0)
      val pclass = if (x(1).contentEquals("")) (Math.random() * 3) else x(1).toDouble
      val pclass_log = math.log(pclass + 0.1)
      val sex = if (x(3).contentEquals("male")) 1.0 else if (x(3).contentEquals("female")) 0.0 else sexFlag //取人数最少的性别
      val sex_log = math.log(sex + 0.1)
      val age = if (x(4).isEmpty) (Math.random() * 100) else x(4).toDouble //随机取年龄值
      val age_log = math.log(age + 0.1)
      val sibSp = if (x(5).contentEquals("")) (Math.random() * 8) else x(5).toDouble
      val sibSp_log = math.log(sibSp + 0.1)
      val parch = if (x(6).contentEquals("")) (Math.random() * 6) else x(6).toDouble
      val parch_log = math.log(parch + 0.1)
      //      val ticket = x(8).toDouble
      val fare = if (x(8).contentEquals("")) fareFlag else x(8).toDouble
      val fare_log = math.log(fare + 0.1)
      //      val cabin =
      val embarked = if (x.length <= 10) Math.random() * 2 else if (x(10).contentEquals("C")) 0.0 else if (x(10).contentEquals("S")) 1.0 else 2.0
      val embarked_log = math.log(embarked + 0.1)
      val features: Array[Double] = Array(passengerId.toDouble, pclass.toDouble, pclass_log.toDouble, sex.toDouble, sex_log.toDouble, age.toDouble, age_log.toDouble,
        sibSp.toDouble, sibSp_log.toDouble, parch.toDouble, parch_log.toDouble, fare.toDouble, fare_log.toDouble, embarked.toDouble, embarked_log.toDouble)
      Vectors.dense(features)
    })
  }

  def loadModel(path: String): Unit = {
    //    val sameModel = RandomForestModel.load(sc, path)
    //    println("Learned classification forest model:\n" + sameModel.toDebugString)
  }

  def trainAndPredict(): Unit = {
    //注意实现要将train.csv和test.csv中的name字段中的逗号替换
    //第一行的标题也要删除
    val org_data = sc.textFile("test/src/main/resources/train.csv").map(line => line.split(","))

    val sexCount = org_data.map(x => (x(4), 1)).reduceByKey(_ + _).map(x => (x._1, x._2)).sortBy(_._2, false).first()._1
    val sexFlag = if (sexCount.contentEquals("male")) 0.0 else if (sexCount.contentEquals("female")) 1.0 else Math.random() * 1
    val fareFlag = org_data.map(x => {
      if (!x(9).contentEquals("")) x(9).toDouble else 0
    }).mean()
    val trainingData = getTrainData(org_data, sexFlag, fareFlag)
    // define parameters
    val paramMap = List(
      "eta" -> 0.1f,
      "max_depth" -> 5,
      "nthread" -> 1,
      "objective" -> "binary:logistic").toMap
    // number of iterations
    val numRound = 1000
    // train the model
    val model = XGBoost.train(trainingData, paramMap, numRound, nWorkers = 5, useExternalMemory = true)

    // Save and load model
    //    model.saveModelAsHadoopFile("target/tmp/xgboost4jModel")(sc)

    // run prediction
    val test_data = sc.textFile("test/src/main/resources/test.csv").map(line => line.split(","))
    val testData = getTestData(test_data, sexFlag, fareFlag)
    val predTrain = model.predict(testData)
    //    predTrain.foreach(x=>x.foreach(println))
    //    val label = predTrain.map(x=>if(x(0)>0.4)1 else 0)
    //    label.foreach(println)
    //    labelAndPreds.coalesce(1).map(x => x._1.toInt + "," + x._2.toInt).saveAsTextFile("target/test.csv")
  }

  def main(args: Array[String]) {
    trainAndPredict()
    //        loadModel("target/tmp/myRandomForestClassificationModel")
  }

}
