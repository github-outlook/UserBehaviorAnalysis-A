package com.githubzhu.logindetect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.util.Collector
import sun.awt.TimedWindowEvent

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/5 16:57
 * @ModifiedBy:
 *
 */
// 定义输入输出样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)

case class LoginFailWarring(userId: Long, firstFailTime: Long, lastFailTime: Long, msg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")

    val loginEventStram: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map(data => {
        val datArr: Array[String] = data.split(",")
        LoginEvent(datArr(0).toLong, datArr(1), datArr(2), datArr(3).toLong)
      })

    //自定义ProcessFunction ，通过注册定时器来判断2s 内连续登陆失败 的需求
    val resStream: DataStream[LoginFailWarring] = loginEventStram
      .keyBy(_.userId)
      .process(new LoginFailDetectWarning(2))

    resStream.print()
    env.execute("login-fail-job")


  }
}

class LoginFailDetectWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarring] {
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail", classOf[LoginEvent]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-Ts", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarring]#Context, out: Collector[LoginFailWarring]): Unit = {
    /* //每个数据判断是否登陆失败
     if (value.eventType == "fail") { //登陆失败添加到list
       loginFailListState.add(value)
       if (timerTsState.value() == 0) { //定时器时间戳为0 ，注册 2s 后定时器
         val ts = value.timestamp * 1000 + 2000L
         ctx.timerService().registerEventTimeTimer(ts)
         timerTsState.update(ts)
       }
     } else {
       ctx.timerService().deleteEventTimeTimer(timerTsState.value())
       loginFailListState.clear()
       timerTsState.clear()
     }*/

    //改进
    if (value.eventType == "fail") { //
      import scala.collection.JavaConversions._
      val failListItr = loginFailListState.get().iterator()
      //      val lastFailTs: Long =failList.last.timestamp
      //      val curFailTs : Long= value.timestamp
      //      loginFailListState.add(value)

      if (failListItr.hasNext) {
        val lastFailTs: Long = failListItr.next().timestamp
        val curFailTs: Long = value.timestamp
        if (curFailTs - lastFailTs < 2) {
          out.collect(LoginFailWarring(ctx.getCurrentKey, lastFailTs, curFailTs, "login fail in 2s for " + loginFailListState.get().size + " times."))
        }
        loginFailListState.add(value)
      } else {
        loginFailListState.add(value)
      }


    } else {
      loginFailListState.clear()
    }


  }

  /*

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarring]#OnTimerContext, out: Collector[LoginFailWarring]): Unit = {
      //定时器触发时，说明没有成功事件来，统计所有的失败事件个数，如果大于设定值就报警
      import scala.collection.JavaConversions._
      val list: List[LoginEvent] = loginFailListState.get().toList
      if (list.size >= 2)
        out.collect(LoginFailWarring(ctx.getCurrentKey, list.head.timestamp, list.last.timestamp, "login fail in 2s for " + list.length + " times."))
      loginFailListState.clear()
      timerTsState.clear()
    }
  */


}


