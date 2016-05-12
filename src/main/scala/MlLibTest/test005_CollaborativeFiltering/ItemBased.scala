package com.jd.Controller

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Lonica on 16/5/9.
 * reference: http://mlnick.github.io/blog/2013/04/01/movie-recommendations-and-more-with-spark/
 * nohup spark-submit --master yarn-client --driver-memory 16g --executor-memory 4g  --num-executors 100 --executor-cores 2 --class com.jd.jddp.dm.model.ItemBased recommendation-model-1.0-SNAPSHOT.jar > nohup_item_based.out &
 */
object ItemBased {
  val PRIOR_COUNT = 10
  val PRIOR_CORRELATION = 0

  val sparkConf = new SparkConf().setAppName("cf item-based").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)

  def doCalcALL(): Unit ={


    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val oriRatings = hiveContext.sql("select usrid, product_id, ordercount from tmp.temp_user_product")
      .rdd.map(k => (k.getAs[Long]("usrid"), k.getAs[Int]("product_id"), k.getAs[Long]("ordercount")))

    //    val g = hiveContext.sql("select usrid, collect_list(product_id), sum(ordercount) from tmp.temp_user_product group by usrid")

    val ratings = oriRatings.groupBy(k => k._1).flatMap(x => (x._2.toList.sortWith((x, y) => x._3 > y._3).take(100)))
    //每个用户购买物品按照ratins大小取前100最喜欢的 (user,item,rating)

    val item2manyUser = ratings.groupBy(tup => tup._2)  //按照用商品id group, (item,Iterable(user,item,rating))
    val numRatersPerItem = item2manyUser.map(grouped => (grouped._1, grouped._2.size))  //计算每个item的购买用户个数 (item,num)

    val ratingsWithSize = item2manyUser.join(numRatersPerItem).   //(item,(CompactBuffer(user,item,rating),num))
      flatMap(joined => {
        joined._2._1.map(f => (f._1, f._2, f._3, joined._2._2))   //(user,item,rating,num)
      })

    // ratingsWithSize now contains the following fields: (user, item, rating, numRaters).
    //(user,(user, item, rating, numRaters))
    val ratings2 = ratingsWithSize.keyBy(tup => tup._1)

    // join on userid and filter item pairs such that we don't double-count and exclude self-pairs
    // user内笛卡尔积，计算两两物品的组合
    //***计算半矩阵，减少计算量
    val ratingPairs = ratings2.join(ratings2).filter(f => f._2._1._2 < f._2._2._2)  // RDD[(user, ((user, item, rating, numRaters), (user, item, rating, numRaters)))]

    // compute raw inputs to similarity metrics for each item pair

    val tempVectorCalcs =
      ratingPairs.map(data => {
        val key = (data._2._1._2, data._2._2._2)  //(item,item)
        val stats =
          (data._2._1._3 * data._2._2._3, // rating 1 * rating 2
            data._2._1._3, // rating item 1
            data._2._2._3, // rating item 2
            math.pow(data._2._1._3, 2), // square of rating item 1
            math.pow(data._2._2._3, 2), // square of rating item 2
            data._2._1._4, // number of raters item 1
            data._2._2._4) // number of raters item 2
        (key, stats)
      })
    val vectorCalcs = tempVectorCalcs.groupByKey().map(data => {  //计算两个物品之间，RDD[((Int, Int), Iterable[(Int, Int, Int, Double, Double, Int, Int)])]
    val key = data._1
      val vals = data._2
      val size = vals.size
      val dotProduct = vals.map(f => f._1).sum
      val ratingSum = vals.map(f => f._2).sum
      val rating2Sum = vals.map(f => f._3).sum
      val ratingSq = vals.map(f => f._4).sum
      val rating2Sq = vals.map(f => f._5).sum
      val numRaters = vals.map(f => f._6).max
      val numRaters2 = vals.map(f => f._7).max
      (key, (size, dotProduct, ratingSum, rating2Sum, ratingSq, rating2Sq, numRaters, numRaters2))
    })
    //.filter(x=>x._2._1>1)

    //补齐另一半矩阵
    val inverseVectorCalcs = vectorCalcs.map(x => ((x._1._2, x._1._1), (x._2._1, x._2._2, x._2._4, x._2._3, x._2._6, x._2._5, x._2._8, x._2._7)))
    //得到总的矩阵你（对称矩阵）
    val vectorCalcsTotal = vectorCalcs ++ inverseVectorCalcs

    // compute similarity metrics for each item pair
    // modify formula as : cosSim *size/(numRaters*math.log10(numRaters2+10))
    val tempSimilarities =
      vectorCalcsTotal.map(fields => {
        val key = fields._1
        val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields._2
        val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq)) * size / (numRaters * math.log10(numRaters2 + 10))
        //        val corr = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
        //        val regCorr = regularizedCorrelation(size, dotProduct, ratingSum, rating2Sum,
        //          ratingNormSq, rating2NormSq, PRIOR_COUNT, PRIOR_CORRELATION)
        //        val jaccard = jaccardSimilarity(size, numRaters, numRaters2)
        (key._1, (key._2, cosSim))
      })

    val similarities = tempSimilarities.groupByKey().flatMap(x => {
      x._2.map(temp => (x._1, (temp._1, temp._2))).toList.sortWith((a, b) => a._2._2 > b._2._2).take(50)
    })

    val similarTable = similarities.map(x => (x._1, x._2._1, x._2._2)).toDF()
    hiveContext.sql("use tmp")
    //    similarTable.insertInto("similar_item_test", true)
    similarTable.write.mode(SaveMode.Overwrite).saveAsTable("tmp.similar_item_test")


    // ratingsInverse format (item,(user,raing))
    val ratingsInverse = ratings.map(rating => (rating._2, (rating._1, rating._3)))

    //  statistics format ((user,item),(sim,sim*rating)),,,, ratingsInverse.join(similarities) fromating as (Item,((user,rating),(item,similar)))
    val statistics = ratingsInverse.join(similarities).map(x => ((x._2._1._1, x._2._2._1), (x._2._2._2, x._2._1._2 * x._2._2._2)))

    // predictResult fromat ((user,item),predict)
    val predictResult = statistics.reduceByKey((x, y) => ((x._1 + y._1), (x._2 + y._2))).map(x => (x._1, x._2._2 / x._2._1))
    //      val predictResult = statistics.reduceByKey((x,y)=>((x._1+y._1),(x._2+y._2))).map(x=>(x._1,x._2._2))

    val filterItem = oriRatings.map(x => ((x._1, x._2), Double.NaN))
    val totalScore = predictResult ++ filterItem

    //      val temp = totalScore.reduceByKey(_+_)

    val finalResult = totalScore.reduceByKey(_ + _).filter(x => !(x._2 equals (Double.NaN))).
      map(x => (x._1._1, x._1._2, x._2)).groupBy(x => x._1).flatMap(x => (x._2.toList.sortWith((a, b) => a._3 > b._3).take(50)))

    //     val aa = finalResult.map(x=>x._1).distinct().count

    val recommendTable = finalResult.toDF()
    hiveContext.sql("use tmp")
    //    recommendTable.insertInto("recommend_item_test", true)
    recommendTable.write.mode(SaveMode.Overwrite).saveAsTable("tmp.recommend_item_test")
  }

  def testCalcSim(): Unit ={

    val oriRatings = sc.parallelize(Seq((1L,1L,2.0),(1L,2L,4.0),(1L,6L,3.0),(1L,10L,5.0),(2L,2L,4.0),(1L,5L,2.0),(2L,10L,5.0),(2L,15L,1.0),(3L,1L,3.0),(3L,8L,5.0),(3L,10L,3.0)))

    //    val g = hiveContext.sql("select usrid, collect_list(product_id), sum(ordercount) from tmp.temp_user_product group by usrid")

    val ratings = oriRatings.groupBy(k => k._1).flatMap(x => (x._2.toList.sortWith((x, y) => x._3 > y._3).take(100)))
    //每个用户购买物品按照ratins大小取前100最喜欢的 (user,item,rating)

    val item2manyUser = ratings.groupBy(tup => tup._2)  //按照用商品id group, (item,Iterable(user,item,rating))
    val numRatersPerItem = item2manyUser.map(grouped => (grouped._1, grouped._2.size))  //计算每个item的购买用户个数 (item,num)

    val ratingsWithSize = item2manyUser.join(numRatersPerItem).   //(item,(CompactBuffer(user,item,rating),num))
      flatMap(joined => {
        joined._2._1.map(f => (f._1, f._2, f._3, joined._2._2))   //(user,item,rating,num)
      })

    // ratingsWithSize now contains the following fields: (user, item, rating, numRaters).
    //(user,(user, item, rating, numRaters))
    val ratings2 = ratingsWithSize.keyBy(tup => tup._1)

    // join on userid and filter item pairs such that we don't double-count and exclude self-pairs
    // user内笛卡尔积，计算两两物品的组合
    //***计算半矩阵，减少计算量
    val ratingPairs = ratings2.join(ratings2).filter(f => f._2._1._2 < f._2._2._2)  // RDD[(user, ((user, item, rating, numRaters), (user, item, rating, numRaters)))]

    // compute raw inputs to similarity metrics for each item pair

    val tempVectorCalcs =
      ratingPairs.map(data => {
        val key = (data._2._1._2, data._2._2._2)  //(item,item)
        val stats =
          (data._2._1._3 * data._2._2._3, // rating 1 * rating 2
            data._2._1._3, // rating item 1
            data._2._2._3, // rating item 2
            math.pow(data._2._1._3, 2), // square of rating item 1
            math.pow(data._2._2._3, 2), // square of rating item 2
            data._2._1._4, // number of raters item 1
            data._2._2._4) // number of raters item 2
        (key, stats)
      })
    val vectorCalcs = tempVectorCalcs.groupByKey().map(data => {  //计算两个物品之间，RDD[((Int, Int), Iterable[(Int, Int, Int, Double, Double, Int, Int)])]
    val key = data._1
      val vals = data._2
      val size = vals.size
      val dotProduct = vals.map(f => f._1).sum
      val ratingSum = vals.map(f => f._2).sum
      val rating2Sum = vals.map(f => f._3).sum
      val ratingSq = vals.map(f => f._4).sum
      val rating2Sq = vals.map(f => f._5).sum
      val numRaters = vals.map(f => f._6).max
      val numRaters2 = vals.map(f => f._7).max
      (key, (size, dotProduct, ratingSum, rating2Sum, ratingSq, rating2Sq, numRaters, numRaters2))
    })
    //.filter(x=>x._2._1>1)

    //补齐另一半矩阵
    val inverseVectorCalcs = vectorCalcs.map(x => ((x._1._2, x._1._1), (x._2._1, x._2._2, x._2._4, x._2._3, x._2._6, x._2._5, x._2._8, x._2._7)))
    //得到总的矩阵你（对称矩阵）
    val vectorCalcsTotal = vectorCalcs ++ inverseVectorCalcs

    // compute similarity metrics for each item pair
    // modify formula as : cosSim *size/(numRaters*math.log10(numRaters2+10))
    val tempSimilarities =
      vectorCalcsTotal.map(fields => {
        val key = fields._1
        val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields._2
        val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq)) * size / (numRaters * math.log10(numRaters2 + 10))
        /*val corr = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
        val regCorr = regularizedCorrelation(size, dotProduct, ratingSum, rating2Sum,
          ratingNormSq, rating2NormSq, PRIOR_COUNT, PRIOR_CORRELATION)
        val jaccard = jaccardSimilarity(size, numRaters, numRaters2)*/
        (key._1, (key._2, cosSim))
      })

    val similarities = tempSimilarities.groupByKey().flatMap(x => {
      x._2.map(temp => (x._1, (temp._1, temp._2))).toList.sortWith((a, b) => a._2._2 > b._2._2).take(50)
    })

    val similarTable = similarities.map(x => (x._1, x._2._1, x._2._2))
    similarTable.foreach(println)

  }

  def main(args: Array[String]) {
    testCalcSim()
  }

  // *************************
  // * SIMILARITY MEASURES
  // *************************

  /**
   *  Pearson correlation
   * The correlation between two vectors A, B is
   * cov(A, B) / (stdDev(A) * stdDev(B))
   *
   * This is equivalent to
   * [n * dotProduct(A, B) - sum(A) * sum(B)] /
   * sqrt{ [n * norm(A)^2 - sum(A)^2] [n * norm(B)^2 - sum(B)^2] }
   */
  def correlation(size: Double, dotProduct: Double, ratingSum: Double,
                  rating2Sum: Double, ratingNormSq: Double, rating2NormSq: Double) = {

    val numerator = size * dotProduct - ratingSum * rating2Sum
    val denominator = scala.math.sqrt(size * ratingNormSq - ratingSum * ratingSum) *
      scala.math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)

    numerator / denominator
  }

  /**
   * Regularize correlation by adding virtual pseudocounts over a prior:
   * RegularizedCorrelation = w * ActualCorrelation + (1 - w) * PriorCorrelation
   * where w = # actualPairs / (# actualPairs + # virtualPairs).
   */
  def regularizedCorrelation(size: Double, dotProduct: Double, ratingSum: Double,
                             rating2Sum: Double, ratingNormSq: Double, rating2NormSq: Double,
                             virtualCount: Double, priorCorrelation: Double) = {

    val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
    val w = size / (size + virtualCount)

    w * unregularizedCorrelation + (1 - w) * priorCorrelation
  }

  /**
   * The cosine similarity between two vectors A, B is
   * dotProduct(A, B) / (norm(A) * norm(B))
   */
  def cosineSimilarity(dotProduct: Double, ratingNorm: Double, rating2Norm: Double) = {
    dotProduct / (ratingNorm * rating2Norm)
  }

  /**
   * The Jaccard Similarity between two sets A, B is
   * |Intersection(A, B)| / |Union(A, B)|
   */
  def jaccardSimilarity(usersInCommon: Double, totalUsers1: Double, totalUsers2: Double) = {
    val union = totalUsers1 + totalUsers2 - usersInCommon
    usersInCommon / union
  }

}