package MlLibTest.test003_ClassifyAndRegress

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by zhangwj on 16-11-1.
 * 自己实现的一些评价指标
 *
 */
object MyEvaluator{

  def getMSE(origin:List[Double],predict:List[Double]): Double ={
    if(origin.length!=predict.length)return -1
    val data = origin.zip(predict)
    var sum:Double = 0
    for(d<-data){
      sum+=math.pow(d._1-d._2,2)
    }
    sum/data.length
  }

  def getRMSE(origin:List[Double],predict:List[Double]): Double ={
    /*
    * rmse =sqrt(mse)
    *
    * */
    var mse = getMSE(origin,predict)
    if(mse.toInt.equals(-1))return -1
    math.sqrt(mse/origin.length)
  }

  def getMAE(origin:List[Double],predict:List[Double]):Double={
    val data = origin.zip(predict)
    var sum:Double = 0
    for(d<-data){
      sum+=math.abs(d._1-d._2 )
    }
    sum/data.length
  }

  /**
   *
   * @param origin original labels
   * @param predict predict labels
   * @return confusion matrix, type = Map[String, Double]
   *
   *  Confusion matrix
   *                                 Predicted
                            Negative              Positive
               Negative         a                    b
      Actual
               Positive         c                    d
   *
   *   AC（Accuracy）=(a+d)/(a+b+c+d)
        TP（recall or true positive rate） = d/(c+d)
        FP（false positive rate） = b/(a+b)
        TN （true negative rate）= a/(a+b)
        FN（false negative rate） = c/(c+d)
        P（precision ） = d/(b+d)
   *
   *
   */
  def getConfusionMatrix(origin:List[Int],predict:List[Int]): mutable.Map[String, Double] ={
    if(origin.length!=predict.length)return null
    var clssNum = origin.distinct
    clssNum = clssNum.sortBy(x=>x) //the smallest is negative classs
    if(clssNum.length!=2){
      println("number of class not equal to 2!")
      return null
    }
    val data = origin.zip(predict)
    var a,b,c,d = 0
    for(elem<-data){
      a += (if(clssNum(0)==elem._1 && elem._1==elem._2)1 else 0)
      b += (if(clssNum(0)==elem._1 && elem._1!=elem._2)1 else 0)
      c += (if(clssNum(1)==elem._1 && elem._1==elem._2)1 else 0)
      d += (if(clssNum(1)==elem._1 && elem._1!=elem._2)1 else 0)
    }
    val cm=scala.collection.mutable.Map("AC"->(a+d).toDouble/(a+b+c+d),
      "TP"->d.toDouble/(c+d),"FP"->b.toDouble/(a+b),"TN"->a.toDouble/(a+b),"FN"->c.toDouble/(c+d),
      "P"->d.toDouble/(b+d))
    cm
  }

  /**
   *
   * @param origin the original label
   * @param score the probability to be positive
   * return ROC points and AUC score
   */
  def getAUC(origin:List[Int],score:List[Double]){
    if(origin.length!=score.length || origin.length==0 || score.length==0)return
    var clssNum = origin.distinct
    clssNum = clssNum.sortBy(x=>x) //the smallest is negative classs
    if(clssNum.length!=2){
      println("number of class not equal to 2!")
      return null
    }
    //get number of positive samples
    var pos_sum = 0
    for(e<-origin)pos_sum += (if(e==clssNum(1))1 else 0)

    val data = origin.zip(score)
    val sorted_data = data.sortBy(_._2).reverse
    var tp1,fp1,tp2,fp2,sum = 0.0
    val N = origin.length
    var points = ListBuffer((0.0,0.0))
    for(elem<-sorted_data){
      tp1=tp2
      fp1=fp2
      if(elem._1==clssNum(1)){
        tp2 += 1
      }else fp2+=1
      sum += tp2*(fp2-fp1)/(pos_sum*(N-pos_sum))
      points += ((fp2/(N-pos_sum),tp2/pos_sum))
    }
    points.foreach(println)
    println(sum)
    (points,sum)
  }

  def main(args: Array[String]) {
    val origin = List(1,1,1,1,1,0,0,0,0,0)
    val predict = List(1,0,1,1,1,1,0,0,0,1)
    println(getConfusionMatrix(origin,predict))
    println(getMSE(origin.map(_.toDouble),predict.map(_.toDouble)))
    println(getRMSE(origin.map(_.toDouble),predict.map(_.toDouble)))
    println(getMAE(origin.map(_.toDouble),predict.map(_.toDouble)))
    val score = List(0.94,0.8,0.73,0.6,0.7,0.3,0.1,0.2,0.01,0.92)
    getAUC(origin,score)  //the result is same as sklearn
  }

}
