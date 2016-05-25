package MlTest.test001_baseTest

/**
 * Created by zhangwj on 16-5-25.
 */
object testUDF {
  val isExist=
    (features:Seq[String],keyword:String)=>{
      if(features==null || features.length==0) false
      for(feature<-features){
        if(feature.contains(keyword)) true
      }
      false
  }
}
