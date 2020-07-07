package test1

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.Random
import org.apache.kafka.clients.producer.ProducerRecord

object Test4_kafkaproducer {
  def main(args: Array[String]): Unit = {
    val topic ="yc74nameaddrphone"
    val props=new Properties
    props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
    props.put("acks", "all")//确认级别
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")//生产端序列化
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    val producer=new KafkaProducer[String,String](props)
    
    val nameAddrs=Map("smith"->"湖南","tom"->"湖北","john"->"湖南")
    val namePhones=Map("smith"->"111111111","tom"->"222222222","john"->"333333333","tim"->"888888888")
    
    val rnd=new Random
    for(nameAddr<-nameAddrs){
      val pr=new ProducerRecord[String,String](topic,nameAddr._1,s"${nameAddr._1}\t${nameAddr._2}\t0")//0表示消息类型 name 地址 类型
      producer.send(pr)
      Thread.sleep(rnd.nextInt(10))
    }
    for( namePhone <- namePhones){
      val pr=new ProducerRecord[String, String](topic, namePhone._1 ,   s"${namePhone._1}\t${namePhone._2}\t1" )   // 1表示消息的类型为  name phone 类型
      producer.send(  pr )
      Thread.sleep(    rnd.nextInt(10 ) )
    }
    print("发送完毕")
    producer.close()

  }
}