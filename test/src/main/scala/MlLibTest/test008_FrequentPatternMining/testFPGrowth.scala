package MlLibTest.test008_FrequentPatternMining

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-4-21.
 * 关联规则：https://en.wikipedia.org/wiki/Association_rule_learning
 * FP-Growth论文：Han et al., Mining frequent patterns without candidate generation （FP stands for frequent pattern.）
 * spark实现并行版PFP: dl.acm.org/citation.cfm?doid=1454008.1454027
 * well known algorithms are Apriori, Eclat and FP-Growth, spark.mllib provides a parallel implementation of FP-growth, a popular algorithm to mining frequent itemsets.
 */
object testFPGrowth {

  val conf = new SparkConf().setMaster("local[2]").setAppName("testPFP")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val data = sc.textFile("src/main/resources/sample_fpgrowth.txt")
    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))
    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)
    val model = fpg.run(transactions) //run参数类型为RDD[Array[Item]]，Item是一个泛型类型

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
    val minConfidence = 0.8
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }
}
