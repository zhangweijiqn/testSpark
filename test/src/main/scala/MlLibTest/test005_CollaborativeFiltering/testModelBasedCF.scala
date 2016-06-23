package MlLibTest.test005_CollaborativeFiltering

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

/**
 * Created by zhangwj on 16-4-6.
 * spark.mllib currently supports model-based collaborative filtering, in which users and products are described by a small set of latent factors that can be used to predict missing entries.
 * 目前支持基于模型的协同过滤，LFM算法，论文出处：http://ieeexplore.ieee.org/xpl/articleDetails.jsp?arnumber=4781121
 * spark.mllib uses the alternating least squares (ALS) algorithm to learn these latent factors.
 * 使用ALS（alternating least squares，交替最小二乘法）算法学习latent factors，论文出处：http://link.springer.com/chapter/10.1007%2F978-3-540-68880-8_32
 * ALS spark实现参考：http://www.csdn.net/article/2015-05-07/2824641
 *  LFM算法
 *
 * 测试数据主要用来处理隐式反馈数据
 */
object testModelBasedCF {
  val conf = new SparkConf().setMaster("local[2]").setAppName("testCF")
  val sc = new SparkContext(conf)
  sc.setCheckpointDir("test/target/checkpoint")

  def main(args: Array[String]) {

    // Load and parse the data
    val data = sc.textFile("test/src/main/resources/als_test.data")
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) => //match应用在map里
      //读入的格式为一个3元组，格式(user,item,ratings),相当于每个元组为原matrix的一个元素
      //要求用户和产品ID都是数值型，并且都是32位非负整数，也就是id不能大于 Integer.MAX_VALUE  2,147,483,647 大约21亿
      Rating(user.toInt, item.toInt, rate.toDouble)
    }).cache()    //ALS算法是迭代的，如果没有cache会每次要用到 RDD 时都需要从原始数据中重新计算。

    //注意rating要求的id的格式是Int，Integer.MAX_VALUE= 2147483647, userId和itemId不能超过这个值的大小。


    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 100
    /*  rank： is the number of latent factors in the model.
        iterations： is the number of iterations to run.
        lambda： specifies the regularization parameter in ALS.
    */
    val model = ALS.train(ratings, rank, numIterations, 0.01,-1,0)//返回的model类型 MatrixFactorizationModel，设定了seed参数（最后一个参数）每次执行结果就一致了
    //也可以自己new ALS对象，然后调用setSeed方法来设置seed
    //得到 model.productFeatures, model.userFeatures

    model.userFeatures.mapValues(_.mkString(",")).foreach(println)

    //输出每个product计算得到的10个features
    model.productFeatures.mapValues(_.mkString(",")).foreach(println)

    // Evaluate the model on rating data
    val userProducts = ratings.map{case Rating(user,product,rate)=>(user,product)}.++(sc.parallelize(Seq((1,3),(4,3))))   //数据集故意去掉了这两个，留作预测
    val predictions = model.predict(userProducts).map{case Rating(user,product,rate)=>((user,product),rate)}
    println("result size:"+predictions.count)
    predictions.foreach(println)
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)   //join是PairRDDFunctions的方法，RDD伴生对象中提供了rddToPairRDDFunctions的隐式转换方法
    // (k,v1)和(k,v2)合并为(k,(v1,v2))

    ratesAndPreds.foreach(println)  //join后14个元素

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
      }.mean()
    println("Mean Squared Error = " + MSE)

    // Save and load model
    model.save(sc, "target/tmp/myCollaborativeFilter")
    val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")


  }
}
