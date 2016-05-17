import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhangwj on 16-5-16.
 */
object testSQL {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("testSQL")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)//需要添加hive依赖包，否则会报class not found的错误
    val df = hiveContext.sql("select * from testdb_001.employees limit 5")
    df.rdd.foreach(println)
  }
}
