package MlLibTest.test004_Cluster

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-4-7.
 * LDA（Latent Dirichlet Allocation）隐含狄利克雷分布，是一种文档主题生成模型。
 * LDA可以用来从文档中提取主题，每个主题在单词概率上的分布
 */
object testLDA {
  val conf = new SparkConf().setAppName("testKMeans").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    //we load word count vectors representing a corpus of documents.LDA以词频向量表示的文档集合作为输入，一行是一个文档
     val data = sc.textFile("test/src/main/resources/sample_lda_data.txt")
     val parsedData = data.map(row=>Vectors.dense(row.trim.split(" ").map(_.toDouble)))
    //RDD[(Long, Vector)]，其中：Long为文章ID，Vector为文章分词后的词向量

    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()
    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(3).setOptimizer("em").run(corpus)
    //LDA支持不同的寻优算法，可以通过setOptimizer来设置(em or online)，包括EM算法EMLDAOptimizer 和OnlineLDAOptimizer
    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")  //vocabSize单词总数量
    val topics = ldaModel.topicsMatrix  //x*k，每行是一个单词，每列是一个topic
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
      println()
    }
    // Topic 0: 9.650313050798523 5.024875899615125 2.5457097554749915 22.248016128268922 7.958463311070493 7.162379649951881 13.270300186957057 2.4094124413425173 1.7610959970350113 4.827071614722209 9.752625452216378
    // Topic 1: 9.290931178166954 14.604020333362659 2.1982204581574027 11.176355206878293 9.896770958437191 9.219782765551649 9.417235188379808 4.995211978758768 1.998728609930056 10.217947031617856 3.74936901642119
    // Topic 2: 7.058755771034524 9.37110376702222 7.256069786367606 6.575628664852786 7.144765730492315 5.6178375844964705 8.312464624663134 2.5953755798987164 4.240175393034933 8.954981353659937 19.498005531362434

    //对每个topic，每个主题是单词上的概率分布，按照概率由大到小排序
    val topicTerms = ldaModel.describeTopics()  //ldaModel.describeTopics(5)，可以指定最相关的前m个单词
    topicTerms.foreach{x=>
      val termIndex_probability = x._1.zip(x._2)
      termIndex_probability.foreach(y=>print(y._1+":"+y._2+" "))
      println
    }
    //3:0.34790616420230686 10:0.2850943959113261 6:0.12376445850380331 9:0.05595183171050021 0:0.05497838366718571 8:0.030978783780049896 1:0.028980455972281396 2:0.027224414927777503 4:0.024718661115113137 5:0.014012759990321543 7:0.006389690219334281
    //4:0.14428500562419944 1:0.14080707507661835 0:0.1313710992214863 5:0.12570813254644436 6:0.11928517703194645 9:0.11863937799947995 7:0.07048866281180675 3:0.05629929951811524 2:0.03938758808484351 10:0.02857870515618013 8:0.02514987692887955
    //1:0.1696508014697504 4:0.12350050022706974 5:0.11815784970287743 0:0.11619242662815156 6:0.11436830774574251 9:0.10437879278814538 2:0.07303251280973949 10:0.05809787640117307 3:0.04616077577148703 7:0.04026162777327158 8:0.036198528682591845

    val docConcentration = ldaModel.docConcentration
    docConcentration.foreachActive((x,y)=>println(x+":"+y))

    // Save and load model.
    ldaModel.save(sc, "target/tmp/cluster/myLDAModel")
    val sameModel = DistributedLDAModel.load(sc, "target/tmp/cluster/myLDAModel")

  }
}
