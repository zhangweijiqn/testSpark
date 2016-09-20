package XGBoost

import ml.dmlc.xgboost4j.scala.{XGBoost, DMatrix}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import ml.dmlc.xgboost4j.LabeledPoint
/**
 * Created by zhangwj on 16-9-20.
 */
object testTitanicScala {
  val conf = new SparkConf().setAppName("testTitanic").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def getTrainData(data: RDD[Array[String]], sexFlag: Double, fareFlag: Double): Iterator[LabeledPoint] = {
    data.map(x => {
      val survival = x(1)
      val passengerId = x(0)
      val pclass = if (x(2).contentEquals("")) (Math.random() * 3) else x(2).toFloat
      val pclass_log = math.log(pclass+0.1)
      val sex = if (x(4).contentEquals("male")) 1.0 else if (x(4).contentEquals("female")) 0.0 else sexFlag //取人数最少的性别
      val sex_log = math.log(sex+0.1)
      val age = if (x(5).isEmpty) (Math.random() * 100) else x(5).toFloat //随机取年龄值
      val age_log = math.log(age+0.1)
      val sibSp = if (x(6).contentEquals("")) (Math.random() * 8) else x(6).toFloat
      val sibSp_log = math.log(sibSp+0.1)
      val parch = if (x(7).contentEquals("")) (Math.random() * 6) else x(7).toFloat
      val parch_log = math.log(parch+0.1)
      //      val ticket = x(8).toFloat
      val fare = if (x(9).contentEquals("")) fareFlag else x(9).toFloat
      val fare_log = math.log(fare+0.1)
      //      val cabin =
      val embarked = if (x.length <= 11) Math.random() * 2 else if (x(11).contentEquals("C")) 0.0 else if (x(11).contentEquals("S")) 1.0 else 2.0
      val embarked_log = math.log(embarked+0.1)
      val features: Array[Float] = Array(passengerId.toFloat,pclass.toFloat,pclass_log.toFloat, sex.toFloat,sex_log.toFloat, age.toFloat,age_log.toFloat,
        sibSp.toFloat,sibSp_log.toFloat, parch.toFloat,parch_log.toFloat, fare.toFloat,fare_log.toFloat, embarked.toFloat,embarked_log.toFloat)
      LabeledPoint.fromDenseVector(survival.toFloat, features)
    }).toLocalIterator
  }

  def getTestData(data: RDD[Array[String]], sexFlag: Double, fareFlag: Double) = {
    data.map(x => {
      val passengerId = x(0)
      val pclass = if (x(1).contentEquals("")) (Math.random() * 3) else x(1).toFloat
      val pclass_log = math.log(pclass+0.1)
      val sex = if (x(3).contentEquals("male")) 1.0 else if (x(3).contentEquals("female")) 0.0 else sexFlag //取人数最少的性别
      val sex_log = math.log(sex+0.1)
      val age = if (x(4).isEmpty) (Math.random() * 100) else x(4).toFloat //随机取年龄值
      val age_log = math.log(age+0.1)
      val sibSp = if (x(5).contentEquals("")) (Math.random() * 8) else x(5).toFloat
      val sibSp_log = math.log(sibSp+0.1)
      val parch = if (x(6).contentEquals("")) (Math.random() * 6) else x(6).toFloat
      val parch_log = math.log(parch+0.1)
      //      val ticket = x(8).toFloat
      val fare = if (x(8).contentEquals("")) fareFlag else x(8).toFloat
      val fare_log = math.log(fare+0.1)
      //      val cabin =
      val embarked = if (x.length <= 10) Math.random() * 2 else if (x(10).contentEquals("C")) 0.0 else if (x(10).contentEquals("S")) 1.0 else 2.0
      val embarked_log = math.log(embarked+0.1)
      val features: Array[Float] = Array(passengerId.toFloat,pclass.toFloat,pclass_log.toFloat, sex.toFloat,sex_log.toFloat, age.toFloat,age_log.toFloat,
        sibSp.toFloat,sibSp_log.toFloat, parch.toFloat,parch_log.toFloat, fare.toFloat,fare_log.toFloat, embarked.toFloat,embarked_log.toFloat)
//      Vectors.dense(features)
      LabeledPoint.fromDenseVector(1, features)
    }).toLocalIterator
  }

  def loadModel(path: String): Unit = {
    val sameModel = RandomForestModel.load(sc, path)
    println("Learned classification forest model:\n" + sameModel.toDebugString)
  }

  def trainAndPredict(): Unit = {
    //注意实现要将train.csv和test.csv中的name字段中的逗号替换
    //第一行的标题也要删除
    val org_data = sc.textFile("test/src/main/resources/train.csv").map(line => line.split(","))

    val sexCount = org_data.map(x => (x(4), 1)).reduceByKey(_ + _).map(x => (x._1, x._2)).sortBy(_._2, false).first()._1
    val sexFlag = if (sexCount.contentEquals("male")) 0.0 else if (sexCount.contentEquals("female")) 1.0 else Math.random() * 1
    val fareFlag = org_data.map(x => {
      if (!x(9).contentEquals("")) x(9).toFloat else 0
    }).mean()
    val trainingData = getTrainData(org_data, sexFlag, fareFlag)
    val train = new DMatrix(trainingData)
    // define parameters
    val paramMap = List(
      "eta" -> 0.1,
      "max_depth" -> 5,
      "objective" -> "binary:logistic").toMap
    // number of iterations
    val round = 1000
    // train the model
    val model = XGBoost.train(train, paramMap, round)


    // Save and load model


    // run prediction
    val test_data = sc.textFile("test/src/main/resources/test.csv").map(line => line.split(","))
    val testData = getTestData(test_data, sexFlag, fareFlag)
    val test = new DMatrix(testData)
    val predTrain = model.predict(test)
    val label = predTrain.map(x=>if(x(0)>0.4)1 else 0)
    label.foreach(println)
//    labelAndPreds.coalesce(1).map(x => x._1.toInt + "," + x._2.toInt).saveAsTextFile("target/test.csv")
  }

  def main(args: Array[String]) {
    trainAndPredict()
    //        loadModel("target/tmp/myRandomForestClassificationModel")
  }
}
