package com.githubzhu.logindetect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/6 17:10
 * @ModifiedBy:
 *
 */


case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, msg: String)

object LoginFailPro {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = getClass.getResource("/LoginLog.csv")
    val dataStream: DataStream[LoginEvent] = env.readTextFile(source.getPath)
      .map(data => {
        val dataArr = data.split(",")
        LoginEvent(dataArr(0).toLong, dataArr(1), dataArr(2), dataArr(3).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
      override def extractTimestamp(element: LoginEvent) = element.timestamp
    })

    val longinFailWaringStream: DataStream[LoginFailWarning] = dataStream
      .keyBy(_.userId)
      .process(new LoginFailDetectWarningPro())

    longinFailWaringStream.print()
    env.execute()

  }
}

class LoginFailDetectWarningPro() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {

  lazy val loginFailState: ValueState[LoginEvent] = getRuntimeContext.getState(new ValueStateDescriptor[LoginEvent]("last-login-fail", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    if (value.eventType == "fail") {
      val lastState: LoginEvent = loginFailState.value()
      if (lastState != null) {
        val lastTs = lastState.timestamp
        val curTs = value.timestamp
        if (curTs - lastTs <= 2) {
          loginFailState.update(value)
          out.collect(LoginFailWarning(value.userId, lastTs, curTs, "login fail"))
        }
      }
      else {
        loginFailState.update(value)
      }
    } else{
      loginFailState.clear()
    }

  }
}