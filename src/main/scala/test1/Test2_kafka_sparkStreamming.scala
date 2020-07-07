package test1

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies

/*
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092,localhost:9093,localhost:9094  --topic  topic74streaming  --from-beginning
 * */
object Test2_kafka_sparkStreamming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //为什么这个线程不能少于2？ 少于二功能无法完整  如读取
    val conf=new SparkConf().setMaster("local[*]").setAppName("networkwordcount")
    val ssc=new StreamingContext(conf,Seconds(2))
    
    //当前是consumer端  要反序列化消息
    val kafkaParams=Map[String,Object](
    "bootstrap.servers"->"node1:9092,node2:9093,node3:9094",
    "key.deserializer"->classOf[StringDeserializer],
    "value.deserializer"->classOf[StringDeserializer],
    "group.id"->"streaming74",//消费者组编号
    "auto.offset.reset"->"latest",  //消息从哪里开始读取 latest从头
    "enable.auto.commit"->(true:java.lang.Boolean) //消息的位移提交方式
    )
    
    
    //要订阅的主题
    val topics=Array("topic74streaming")
    //创建Dstream
    val stream=KafkaUtils.createDirectStream[String,String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](topics,kafkaParams)
    )
        
    
    //ConsumerRecord
    val lines:DStream[String]=stream.map(record=>record.value())
    val words:DStream[String]=lines.flatMap(_.split(" "))
    val wordAndOne:DStream[(String,Int)]=words.map(( _,1))
    val reduced:DStream[(String,Int)]=wordAndOne.reduceByKey(_+_)
    
    reduced.print()
    ssc.start() //启动 程序
    print("线程启动")
    ssc.awaitTermination()//等待系统发出退出信号
  }
}