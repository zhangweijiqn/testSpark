package Kaggle

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD

/**
 * Created by zhangwj on 16-9-7.
 *
 * https://www.kaggle.com/c/titanic/
 */
object testTitanic {

  val conf = new SparkConf().setAppName("testTitanic").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def getTrainData(data:RDD[Array[String]],sexFlag:Double,fareFlag:Double): RDD[LabeledPoint] ={
    data.map(x=>{
      val survival = x(1)
      val passengerId = x(0)
      val pclass = if(x(2).contentEquals(""))(Math.random()*3) else x(2).toDouble
      val sex = if(x(4).contentEquals("male"))1.0 else if(x(4).contentEquals("female"))0.0 else sexFlag //取人数最少的性别
      val age = if(x(5).isEmpty)(Math.random()*100) else x(5).toDouble  //随机取年龄值
      val sibSp = if(x(6).contentEquals(""))(Math.random()*8) else x(6).toDouble
      val parch = if(x(7).contentEquals(""))(Math.random()*6) else x(7).toDouble
      //      val ticket = x(8).toDouble
      val fare = if(x(9).contentEquals(""))fareFlag else x(9).toDouble
      //      val cabin =
      val embarked = if(x.length<=11)Math.random()*2  else if(x(11).contentEquals("C"))0.0 else if(x(11).contentEquals("S")) 1.0 else 2.0
      val features:Array[Double] = Array(passengerId.toDouble,pclass.toDouble,sex.toDouble,age.toDouble,sibSp.toDouble,parch.toDouble,fare.toDouble,embarked.toDouble)
      LabeledPoint(survival.toDouble,Vectors.dense(features))
    })
  }

  def getTestData(data:RDD[Array[String]],sexFlag:Double,fareFlag:Double) ={
    data.map(x=>{
      val passengerId = x(0)
      val pclass = if(x(1).contentEquals(""))(Math.random()*3) else x(1).toDouble
      val sex = if(x(3).contentEquals("male"))1.0 else if(x(3).contentEquals("female"))0.0 else sexFlag //取人数最少的性别
      val age = if(x(4).isEmpty)(Math.random()*100) else x(4).toDouble  //随机取年龄值
      val sibSp = if(x(5).contentEquals(""))(Math.random()*8) else x(5).toDouble
      val parch = if(x(6).contentEquals(""))(Math.random()*6) else x(6).toDouble
      //      val ticket = x(8).toDouble
      val fare = if(x(8).contentEquals(""))fareFlag else x(8).toDouble
      //      val cabin =
      val embarked = if(x.length<=10)Math.random()*2 else if(x(10).contentEquals("C"))0.0 else if(x(10).contentEquals("S")) 1.0 else 2.0
      val features:Array[Double] = Array(passengerId.toDouble,pclass.toDouble,sex.toDouble,age.toDouble,sibSp.toDouble,parch.toDouble,fare.toDouble,embarked.toDouble)
      Vectors.dense(features)
    })
  }

  def loadModel(): Unit ={
    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
    println("Learned classification forest model:\n" + sameModel.toDebugString)
  }

  def trainAndPredict(): Unit ={
    //注意实现要将train.csv和test.csv中的name字段中的逗号替换
    //第一行的标题也要删除
    val org_data = sc.textFile("src/main/resources/train.csv").map(line=>line.split(","))
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

    val sexCount = org_data.map(x=>(x(4),1)).reduceByKey(_+_).map(x=>(x._1,x._2)).sortBy(_._2,false).first()._1
    val sexFlag = if(sexCount.contentEquals("male"))0.0 else if(sexCount.contentEquals("female"))1.0 else Math.random()*1
    val fareFlag = org_data.map(x=>{ if(!x(9).contentEquals(""))x(9).toDouble else 0}).mean()
    val trainingData = getTrainData(org_data,sexFlag,fareFlag)

    import org.apache.spark.mllib.tree.RandomForest
    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 10 // Use more in practice. 10棵树增加到100棵，成绩反而下降
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "entropy"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Save and load model
    model.save(sc, "target/tmp/myRandomForestClassificationModel")

    val test_data = sc.textFile("src/main/resources/test.csv").map(line=>line.split(","))
    //    println("0 "+test_data.first()(0))
    //    println("1 "+org_data.first()(1))
    //    println("2 "+org_data.first()(2))
    //    println("3 "+test_data.first()(3))
    //    println("4 "+test_data.first()(4))
    //    println("5 "+test_data.first()(5))
    //    println("6 "+test_data.first()(6))
    //    println("7 "+test_data.first()(7))
    //    println("8 "+test_data.first()(8))
    //    println("9 "+test_data.first()(9))
    //    println("10 "+test_data.first()(10))
    //    println("11 "+test_data.first()(11))
    val testData = getTestData(test_data,sexFlag,fareFlag)
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point)
      (point(0), prediction)
    }
    labelAndPreds.coalesce(1).map(x=>x._1.toInt+","+x._2.toInt).saveAsTextFile("target/test.csv")
  }

  def main(args: Array[String]) {
//    trainAndPredict()
    loadModel()
  }
}
