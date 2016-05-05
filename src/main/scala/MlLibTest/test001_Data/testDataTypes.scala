package MlLibTest.test001_Data

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.stat.{Statistics, MultivariateStatisticalSummary}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by zhangwj on 16-2-17.
 * https://spark.apache.org/docs/latest/mllib-data-types.html
 */
object testDataTypes {

  //构造函数初始化SparkContext
  val conf = new SparkConf().setAppName("MyTest").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)

  /*本地向量 Vector*/
  def testLocalVector(): Unit ={
    //向量: dense,sparse，unsupervized learning
    import org.apache.spark.mllib.linalg.{Vector, Vectors}

    /*dense vector*/
    // Create a dense vector (1.0, 0.0, 3.0).
    val dv1: Vector = Vectors.dense(1.0, 0.0, 3.0) //dense(firstValue : scala.Double, otherValues : scala.Double*) Double*为不定长元素
    println(dv1)
    val dv2:Vector = Vectors.dense(Array(0.0,1.0,3.0)) //dense(values : scala.Array[scala.Double])
    println(dv2)

    /*sparse vector*/
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    println(sv1)  //(3,[0,2],[1.0,3.0])
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    println(sv2)  //(3,[0,2],[1.0,3.0])
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.   sparse(  size,Map(index:data)  ) , index start from 0
    val sv3 = Vectors.sparse(5,(1 to 5).map( x=> (x-1,x*3.toDouble) ))  // (5,[0,1,2,3,4],[3.0,6.0,9.0,12.0,15.0])
    println(sv3)

  }

  /* 加载文件中的数据到本地向量 RDD[Vector] */
  def testGetRDDVector(): Unit ={
    //获取spark RDD[Vector]
    val data = sc.textFile("src/main/resources/test.txt").map(_.split(" ")).map(_.map(_.toDouble))
    data.foreach(_.foreach(println))
    val observations: RDD[Vector] = data.map(x => Vectors.dense(x))   //需要引入mllib的vector，否则会使用scala vector，编译不通过
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
  }

  /* 向量标签 LabeledPoint */
  def testLabeledPoint(): Unit ={
    //带有label的Local Vector，LabeledPoint用于supervized learning算法
    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.regression.LabeledPoint

    // Create a labeled point with a positive label and a dense feature vector.
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    println(pos)  //(1.0,[1.0,0.0,3.0])

    // Create a labeled point with a negative label and a sparse feature vector.
    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
    //(ElemNum,NoneZero-ElemPos,NoneZero-ElemValue)
    println(neg)
  }

  /* 加载文件中的数据到本地向量标签 RDD[LabeledPoint] */
  def testSparseData(): Unit ={
    //sparse data
    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.util.MLUtils
    import org.apache.spark.rdd.RDD

    //读取libSVM输入样本文件,行格式为LabeledPoint,会将数据中的(index,value)解析为(Integer, Double)类型
    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "src/main/resources/sample_libsvm_data.txt")
    //examples.foreach(println _) //(0.0,(692,[127,128,129,130,131,154,155,156,157,158,159,...],[51.0,159.0,253.0,159.0,50.0,48.0,238.0,252.0,252.0,252.0,237.0,...]
    examples.map{ point =>
      print(point.label+":")    //point.label获取label，point.features获取features
      println(point.features)
    }
    // 获取到的索引比给的索引少1，并且对data中的 Map(indice,data)进行了解析，格式保存为 Local Vector
/*    for(i <- examples){
      println(i)
    }*/
  }

  /* 本地矩阵 Matrix */
  def testLocalMatrix(): Unit ={
    //本地matrix只能存储在一台机器上
    import org.apache.spark.mllib.linalg.{Matrix, Matrices}

    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))  //按列存储

    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    /** sparse参数信息：
     * numRows number of rows
     * numCols number of columns
     * colPtrs the index corresponding to the start of a new column： 非零值元素数组中第几个位置开始新的一列，最后一个值实际上是空列
     * rowIndices the row index of the entry ：非零元素数组各个元素所在的行位置，和数组中元素一一对应
     * values non-zero matrix entries in column major： 非零值元素数组
     */

    /* 貌似Matrix不能还原中间有列为空的情况，必须是严格的sparse data,下面这种情况要用dense data
    *   1 0 6
    *   9 0 8
    * */
    val sm2: Matrix = Matrices.sparse(2, 2, Array(0,2,4), Array(0,1, 0, 1), Array(1,9, 6, 8))
    println(sm2.toString())
    val trans_sm = sm2.transpose
    println(sm2.numNonzeros)
  }

  /*  分布式矩阵 RowMtrix，每行存储在一个机器上*/
  def testRowMatrix():Unit={
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR) ///设置日志级别

    import org.apache.spark.mllib.linalg.distributed.RowMatrix
    import org.apache.spark.mllib.linalg.Vectors
    val rows: RDD[Vector] = sc.parallelize(Seq(Vectors.dense(1,2,3,4),Vectors.dense(5,6,7,8),Vectors.dense(2,3,4,5))) // an RDD of local vectors
    // Create a RowMatrix from an RDD[Vector].
    val mat: RowMatrix = new RowMatrix(rows)  //将RDD[Vector]转换为二维的Matrix

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()
    println("m="+m)
    println("n="+n)

    val columnSimilarity = mat.columnSimilarities();
    println(columnSimilarity.numCols())
    println(columnSimilarity.numRows())
    columnSimilarity.entries.saveAsTextFile("target/test/matrix")  //entries中存储的RDD结构数据

    val cp = mat.computePrincipalComponents(3)  //返回Matrix类型

    val summy : MultivariateStatisticalSummary = mat.computeColumnSummaryStatistics()
    println(summy.mean)//平均数

    //矩阵相乘
    val dm: Matrix = Matrices.dense(4, 2, Array(1.0, 0,8.0,3.0, 5.0, 2.0, 4.0, 6.0))  //按列存储
    val result = mat.multiply(dm)
    result.rows.foreach(println)

    // QR decomposition
//    val qrResult = mat.tallSkinnyQR(true)
  }

  /* 分布式矩阵，带有行索引 */
  def testIndexedRowMatrix(): Unit ={
    import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}

    val rows: RDD[IndexedRow] = sc.parallelize(  Seq( new IndexedRow(1,Vectors.dense(1,2,3,4)),new IndexedRow(2,Vectors.dense(4,2,3,1)) )  ) // an RDD of indexed rows
    // Create an IndexedRowMatrix from an RDD[IndexedRow].
    val mat: IndexedRowMatrix = new IndexedRowMatrix(rows)

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()

    mat.columnSimilarities().entries.foreach(println(_))

    // Drop its row indices.
    val rowMat: RowMatrix = mat.toRowMatrix()
  }

  /* CoordinateMatrix，适用于数据特别稀疏的情况 */
  def testCoordinateMatrix(): Unit ={
    import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

    val entries: RDD[MatrixEntry] = sc.parallelize(Seq( new MatrixEntry(0,0,1),new MatrixEntry(1,0,3),new MatrixEntry(1,1,4),new MatrixEntry(0,2,2)   )) // an RDD of matrix entries
    // Create a CoordinateMatrix from an RDD[MatrixEntry].
    val mat: CoordinateMatrix = new CoordinateMatrix(entries)

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()

    val trans_mat = mat.transpose()

    // Convert it to an IndexRowMatrix whose rows are sparse vectors.
    val indexedRowMatrix = mat.toIndexedRowMatrix()

    transCoordinateMatrixToMatrix(mat)
  }

  /**
   * 提供将CoordinateMatrix转成Matrix格式方法
   * @param mat
   * @return
   */
  def transCoordinateMatrixToMatrix(mat:CoordinateMatrix): Matrix ={
    // coordinateMatrix to Matrix, item*user matrix，按userId排序
    val arrayData = mat.entries.collect()
    val rowArray=ArrayBuffer[Int]()
    val valueArray=ArrayBuffer[Double]()
    val colmArray =ArrayBuffer[Int]()
    var count=0
    var formerValue:Long=0

    arrayData.foreach(
      x=>{
        rowArray+=x.i.toInt
        valueArray+=x.value
        if(count==0){
          colmArray+=0
        }
        else {
          if(x.j!=formerValue)
            colmArray+=count
        }
        formerValue=x.j
        count+=1
      }
    )
    colmArray+=count
    /*colmArray.foreach{
      x=>print(x+"\t")
    }
    println
    rowArray.foreach{
      x=>print(x+"\t")
    }
    println
    valueArray.foreach{
      x=>print(x+"\t")
    }
    println*/
    val sm: Matrix = Matrices.sparse(2,3, colmArray.toArray, rowArray.toArray, valueArray.toArray)

    sm.toArray.foreach(println)
    sm
  }

  def testLoadDatatoMatrix(): Unit ={
    import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
    // Load and parse the data, data format (userSeqNum,featuresSeqNum,value)(可以是product也可以是portraits属性)
    val data =sc.textFile("src/main/resources/als_test.data").map(_.split(","))
    val entries = data.map(x=>MatrixEntry(x(0).toLong,x(1).toLong,x(2).toDouble)).persist()
    val mat: CoordinateMatrix = new CoordinateMatrix(entries)
    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()
    println("m="+m+",n="+n)

    val simMatrx = calcSimilarity(mat.transpose().toRowMatrix())

    val simMatrxRdd = simMatrx.entries

    entries.unpersist()

//    simMatrxRdd.saveAsTextFile("target/test/similarityMatrx")

    //查找编号1相关
    val Corr = simMatrxRdd.filter(x=>x.i==1)
    implicit val KeyOrdering = new Ordering[MatrixEntry] {
      override def compare(x:MatrixEntry, y: MatrixEntry): Int = {
        (x.value*1000 - y.value*1000).toInt
      }
    }
    val top2 = Corr.top(4)
    top2.foreach{
      x=>println(x.j+":"+x.value)
    }
  }

  def calcSimilarity(data:RowMatrix): CoordinateMatrix ={
    val simMatrix = data.columnSimilarities(0.0)
    simMatrix
  }

  def main(args:Array[String]) {
//    testLocalVector
//    testLabeledPoint
//    testSparseData
//    testGetRDDVector
    testLocalMatrix
//    testRowMatrix
//    testIndexedRowMatrix
//    testLoadDatatoMatrix()
//    testCoordinateMatrix()
  }
}
