package XGBoost

/**
 * Created by zhangwj on 16-7-20.
 *
 *  xgboost4j
 *
 * install: https://github.com/dmlc/xgboost/blob/master/doc/build.md
 * get started: https://github.com/dmlc/xgboost/blob/master/doc/get_started/index.md
 * 在源代码的xgboost/jvm-packages/xgboost4j/target路径下有 xgboost生成的jar包
 *
 *  mvn install:install-file -Dfile=xgboost4j-0.5.jar -DgroupId=ml.dmlc -DartifactId=xgboost4j -Dversion=0.5 -Dpackaging=jar
 *
 *  有专门xgboost on spark
 * */

import ml.dmlc.xgboost4j.scala.DMatrix
import ml.dmlc.xgboost4j.scala.XGBoost

object test {
  def main(args: Array[String]) {
    // read trainining data, available at xgboost/demo/data
    val trainData =
      new DMatrix("/home/zhangwj/Applications/XGBoost/xgboost/demo/data/agaricus.txt.train")
    // define parameters
    val paramMap = List(
      "eta" -> 0.1,
      "max_depth" -> 2,
      "objective" -> "binary:logistic").toMap
    // number of iterations
    val round = 2
    // train the model
    val model = XGBoost.train(trainData, paramMap, round)
    // run prediction
    val predTrain = model.predict(trainData)
    predTrain.foreach(println)
    // save model to the file.
    model.saveModel("target/model")
  }
}