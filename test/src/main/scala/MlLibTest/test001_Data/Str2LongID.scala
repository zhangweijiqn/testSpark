package MlLibTest.test001_Data

/**
 * Created by zhangwj on 16-6-26.
 */
object Str2LongID {

  def hashId(str:String) ={
    import com.google.common.hash.Hashing //google guava
    Hashing.md5().hashString(str).asLong()  //64 bit long
  }

  def main(args: Array[String]): Unit = {
     println(hashId("test"))
  }
}
