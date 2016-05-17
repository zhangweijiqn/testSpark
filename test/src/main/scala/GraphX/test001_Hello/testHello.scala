package GraphX.test001_Hello

import org.apache.spark.graphx.{Graph, Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-3-13.
 */
object testHello {



  def main(args: Array[String]) {

//    println(System.getenv("SPARK_HOME"))
    val conf = new SparkConf().setAppName("testGraphX").setMaster("yarn-client").setSparkHome(System.getenv("SPARK_HOME"))
    //提交任务到yarn-client
    val sc = new SparkContext(conf)

    // Create an RDD for the vertices,创建顶点
    val users: RDD[(VertexId, (String, String))] =  //使用VertexId类型来记录顶点id
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // Create an RDD for edges，创建边
    val relationships: RDD[Edge[String]] =    //Edge接受参数的方式？？？
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    //图操作参考Graph方法

    // Count all users which are postdocs
    val postdocsNum = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    println(postdocsNum)
    // Count all the edges where src > dst
    var Num = graph.edges.filter(e => e.srcId >= e.dstId).count
    println(Num)

    Num = graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count
    println(Num)


  }

}
