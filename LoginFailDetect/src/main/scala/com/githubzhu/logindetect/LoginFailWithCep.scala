package com.githubzhu.logindetect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/6 17:10
 * @ModifiedBy:
 *
 */
object LoginFailWithCep {
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

    //创建一个模式pattern
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("first-fail").where(_.eventType == "fail") //第一次登陆失败事件
      .next("second-fail").where(_.eventType == "fail")
      .within(Time.seconds(2)) //在两秒内连续发生有效


    //将pattern应用到dataStram 得到一个pattern
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(dataStream.keyBy(_.userId), loginFailPattern)

    //检出合乎规则的复杂事件，转换成结果输出
    val loginFailWaringStream: DataStream[LoginFailWarning] = patternStream
      .select(new LoginFailDetect())


    //循环模式定义实例2
    val loginFailPattern2 = Pattern
      .begin[LoginEvent]("fails").times(2).where(_.eventType=="fail").consecutive()
      .within(Time.seconds(10))

    val patternStream2:PatternStream[LoginEvent]=  CEP.pattern(dataStream,loginFailPattern2)

    val loginFailWaringStream2 = patternStream2
        .select(new LoginFailDetect2())


    loginFailWaringStream2.print("login-F2")
    loginFailWaringStream.print()
    env.execute()

  }

}

class LoginFailDetect() extends PatternSelectFunction[LoginEvent, LoginFailWarning] {

  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {

    val firstFailEvent = map.get("first-fail").iterator().next
    val secondFailEvent = map.get("second-fail").iterator().next

    LoginFailWarning(firstFailEvent.userId,firstFailEvent.timestamp,secondFailEvent.timestamp,"login-fail")
  }
}


class LoginFailDetect2() extends PatternSelectFunction[LoginEvent,LoginFailWarning] {

  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {

    val firstFail: LoginEvent = map.get("fails").get(0)
    val secondFailEvent :LoginEvent = map.get("fails").get(1)


    LoginFailWarning(firstFail.userId,firstFail.timestamp,secondFailEvent.timestamp,"login-fail")
  }
}