package test1

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Test1_socketStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //为什么这个线程不能少于2？
    val conf=new SparkConf().setMaster("local[*]").setAppName("networkwordcount")
    val ssc=new StreamingContext(conf,Seconds(2))
    
    //实时流监听的是 localhost,9999端口
    val lines:ReceiverInputDStream[String]=ssc.socketTextStream("localhost", 9999)
  
    //进行单词计数
    val words=lines.flatMap(_.split(" "))
    val pairs=words.map(word=>(word,1))
    val wordcounts=pairs.reduceByKey( _+_)
    
    wordcounts.print()
    
    ssc.start() //启动 程序
    ssc.awaitTermination()//等待系统发出退出信号
  }
}