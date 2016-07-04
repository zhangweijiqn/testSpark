package GraphX.test001_Hello

import org.apache.spark.graphx.{Graph, Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-3-13.
 */
object testHello {

  //    println(System.getenv("SPARK_HOME"))
  val conf = new SparkConf().setAppName("testGraphX").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
  //提交任务到yarn-client
  val sc = new SparkContext(conf)

  def baseTest(): Unit ={
    // Create an RDD for the vertices,创建顶点
    val vertices: RDD[(VertexId, (String, String,Int))] =  //(String, String,Int)类型自定义，使用VertexId类型来记录顶点id
      sc.parallelize(Array((3L, ("rxin", "student",25)), (7L, ("jgonzal", "postdoc",26)),
        (5L, ("franklin", "prof",24)), (2L, ("istoica", "prof",25)),(1L,("","",12))))

    // Create an RDD for edges，创建边
    val edges: RDD[Edge[String]] =    //每条边的类型Edge
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),  //第三个字段类型也自定
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // Build the initial Graph
    val graph = Graph(vertices, edges)

    println("**********************************************************")
    println("联通组件")
    println("**********************************************************")
    val connectedCompoentGraph=graph.connectedComponents()  //返回的是Graph[VertexId, ED]，VertexId是每个顶点所属连通组件的唯一标识符
    //会计算一会，顶点集合，：Array((1,1), (2,2), (3,2), (5,2), (7,2))，第二个元素是一个联通子图的唯一标志符，可以看到有两个联通子图
    val componentCounts = connectedCompoentGraph.vertices.map(_._2).countByValue
    //  Map(1 -> 1, 2 -> 4)，得到联通组件的个数和大小,2
    val sortedCompotents = componentCounts.toSeq.sortBy(_._2).reverse
    //ArrayBuffer((2,4), (1,1))，第一个联通子图4个顶点，第二个一个顶点

    //度的分布
    val degrees = graph.degrees.cache()
    degrees.map(_._2).stats() //(count: 4, mean: 2.000000, stdev: 0.707107, max: 3.000000, min: 1.000000)



    //图操作参考Graph方法

    // Count all users which are postdocs
    val postdocsNum = graph.vertices.filter { case (id, (name, pos,age)) => pos == "postdoc" }.count
    println(postdocsNum)
    // Count all the edges where src > dst
    var Num = graph.edges.filter(e => e.srcId >= e.dstId).count
    println(Num)

    Num = graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count
    println(Num)

  }

  /**
   * 测试顶点id包含重复，边包含重复id
   */
  def testDuplicate(): Unit ={
    val vertices: RDD[(VertexId, (String, String,Int))] =  //(String, String,Int)类型自定义，使用VertexId类型来记录顶点id
      sc.parallelize(Array((3L, ("rxin", "student",25)), (3L, ("rxin", "student",25)),
        (5L, ("franklin", "prof",24)), (2L, ("istoica", "prof",25))))

    // Create an RDD for edges，创建边
    val edges: RDD[Edge[String]] =    //每条边的类型Edge
      sc.parallelize(Array(Edge(3L, 7L, "collab"),Edge(3L, 7L, "collab"),  //第三个字段类型也自定
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val graph = Graph(vertices, edges)
    graph.cache()
    println(graph.vertices.count())   //顶点没有去重
    println(graph.edges.count)    //边也没去重
  }

  def main(args: Array[String]) {

    baseTest()
//
//    testDuplicate()

  }

}
