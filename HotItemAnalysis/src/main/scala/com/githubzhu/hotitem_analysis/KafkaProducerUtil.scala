package com.githubzhu.hotitem_analysis

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/2 21:13
 * @ModifiedBy:
 *
 */
object KafkaProducerUtil {


  def main(args: Array[String]): Unit = {
    writeToKafka("hotitem")
  }

  def writeToKafka(topic: String): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    //创建Kafka producer

    val producer = new KafkaProducer[String, String](properties)
    //从kafaka中读取数据，
    val bufferSource: BufferedSource = io.Source.fromFile("E:\\JavaWorkingSpace\\idea2019.2.3\\Flink\\UserBehaviorAnalysis-A\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")

    for (line <- bufferSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }

}
