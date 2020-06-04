package com.githubzhu.hotitem_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/2 22:32
 * @ModifiedBy:
 *
 */
object HotItemsWithSQL {
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

     tableEnv.createTemporaryView( "dataTable", dataStream,'itemId, 'behavior, 'timestamp.rowtime as 'ts)


    val resTable = tableEnv.sqlQuery(
      """
        |select *
        |from(
        |select * , row_number() over (partition by windowEnd order by cnt desc) as row_num
        |from(
        |select itemId ,hop_end(ts ,interval '5' minute ,interval '1' hour ) as windowEnd , count(itemId)as cnt
        | from dataTable
        | where behavior = 'pv'
        | group by itemId ,hop(ts,interval '5' minute ,interval '1' hour)
        |   )
        |)
        |where row_num <=5
        |""".stripMargin

    )

    resTable.toRetractStream[Row].print("res")

    env.execute()


  }

}
