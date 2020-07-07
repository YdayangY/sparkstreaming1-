package test1

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Test5_saveAsText {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //为什么这个线程不能少于2？ 少于二功能无法完整  如读取
    val conf=new SparkConf().setMaster("local[*]").setAppName("networkwordcount")
    val ssc=new StreamingContext(conf,Seconds(2))
    
    val input="data/input/"
    val output="data/output/"
  
    val textStream=ssc.textFileStream(input)
    val wcStream=textStream.flatMap{line=>line.split(" ")}.map(word=>(word,1)).reduceByKey(_+_)
    
    wcStream.print()
    
    wcStream.saveAsTextFiles(output+"savedTxtFile")
    wcStream.saveAsObjectFiles(output+"savedObjectFile")
    
    ssc.start()
    ssc.awaitTermination()
  
    
  }
}