package test1

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

object TextFileStream {
  def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
    //为什么这个线程不能少于2？ 少于二功能无法完整  如读取
    val conf=new SparkConf().setMaster("local[*]").setAppName("networkwordcount")
    val ssc=new StreamingContext(conf,Seconds(2))
    
      //checkpoint作为容错的设计，基本思路是把当前运行的状态，保存在容错的存储系统中,对于容错处理，保存的内容包括元数据和数据两部分
      ssc.checkpoint("./chpoint")//window操作与有状态的操作
      val lines=ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY_SER)
      val words=lines.flatMap(x=>x.split(" "))
      //用reducedByKeyAndWindow操作进行叠加处理，窗口时间间隔和滑动时间间隔
      //每10s统计前30s各单词累计出现的次数
      val wordCounts=words.map(word=>(word,1)).reduceByKeyAndWindow((a:Int,b:Int)=>a+b, Seconds(30),Seconds(10))
      
      printValues(wordCounts)
      ssc.start()
      ssc.awaitTermination()
  }
  //定义一个打印函数,打印RDD中所有的元素
  def printValues(stream:DStream[(String,Int)]){//DStream->n个RDD组成 ->一个RDD由n条记录组成 ->一个记录(String,Int)组成
    stream.foreachRDD(foreachFunc)// 不要用foreach() -> foreachRDD
    def foreachFunc=(rdd:RDD[(String,Int)])=>{
      val array=rdd.collect() //采集worker端的结果传到driver端
      println("===begin to show results ===")
      for(res <- array){
        println(res)
      }
      println("=== ending show result ===")
    }
  }
  
}