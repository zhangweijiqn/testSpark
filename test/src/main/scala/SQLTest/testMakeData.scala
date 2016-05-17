package SQLTest

import java.io.PrintWriter

/**
 * Created by zhangwj on 16-3-2.
 */
object testMakeData {
  //  val Number = 10000L
  val RowNumber = 8L
  val ColumNumber = 10L
  def main(args: Array[String]) {
    val out = new PrintWriter("src/main/resources/sample.data")
    var i=0
    var j=0
    var ratings:Double = 0
    while(i<RowNumber){
      while(j<ColumNumber){
        ratings = Math.random()
        if(i*j%3!=0 || i==0){
          println(i+","+j+","+ratings)
          out.println(i+","+j+","+ratings)
        }
        j+=1
      }
      i+=1
      j=0
    }
    out.close()
  }
}
