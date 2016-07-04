package MlLibTest.test001_Data

/**
 * Created by zhangwj on 16-6-26.
 */
object Str2LongID {

  def hashMd5Id(str:String) ={
    //字符串转换为64位 Long型变量。
    import com.google.common.hash.Hashing //google guava
    Hashing.md5().hashString(str).asLong()  //64 bit long
  }

  def hashId(str:String)={
    //字符串产生32位 Int型变量，数量很大的时候发生冲突的可能性也会增大
    import com.google.common.hash.Hashing
    str.hashCode
  }

  def main(args: Array[String]): Unit = {
     println(hashId("test"))
  }
}
