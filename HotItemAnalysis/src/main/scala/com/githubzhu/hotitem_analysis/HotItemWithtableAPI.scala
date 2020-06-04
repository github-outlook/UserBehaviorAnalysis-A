package com.githubzhu.hotitem_analysis

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/2 21:10
 * @ModifiedBy:
 *
 */
object HotItemWithtableAPI {
  def main(args: Array[String]): Unit = {

    //创建一个流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //创建表执行环境
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    //从文件读取数据
    val inputStream: DataStream[String] = env.readTextFile("E:\\JavaWorkingSpace\\idea2019.2.3\\Flink\\UserBehaviorAnalysis-A\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")

    //转换成样例类，并分配时间戳和watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val dataArr: Array[String] = data.split(",")
        UserBehavior(dataArr(0).toLong, dataArr(1) toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)

      }).assignAscendingTimestamps(_.timestamp * 1000)

    //把流直接注册成表，并提取重要字段  ，定义时间属性

    val dataTable: Table = tableEnv.fromDataStream( dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    //调用 TableAPI 进行开窗聚合
    val aggTable: Table = dataTable
      .filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt)


    //聚合结果注册到环境 ，写SQL 实现Top N  的选取
    tableEnv.createTemporaryView("agg",aggTable,'itemId,'cnt,'windowEnd)

    val resTable: Table = tableEnv.sqlQuery(
      """
        |select *
        |from
        |(
        |select * ,row_number() over (partition by windowEnd order by cnt desc) as row_num
        |from agg
        |)
        |where row_num <= 5
        |""".stripMargin
      /*"""
        |select *
        |from(
        |select * ,row_number() over（partition by windowEnd order by cnt desc）as row_num
        |from agg
        |)
        |where row_num <= 5
        |""".stripMargin*/)

  /*  """
      |select *
      |from (
      |  select *, row_number() over (partition by windowEnd order by cnt desc) as row_num
      |  from agg
      |  )
      |where row_num <= 5
      """.stripMargin)*/


    resTable.toRetractStream[Row].print("res")
    aggTable.toAppendStream[Row].print("agg")

    env.execute()

  }
}

case class UserBehavior(userId: Long, itemId: Long, catagoryId: Int, behavior: String, timestamp: Long)