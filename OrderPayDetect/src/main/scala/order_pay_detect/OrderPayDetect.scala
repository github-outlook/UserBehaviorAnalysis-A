package com.githubzhu.order_pay_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/7 9:55
 * @ModifiedBy:
 *
 */
//定义输入输出样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

case class OrderResult(orderId: Long, status: String)

object OrderPayDetect {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/OrderLog.csv")
    val inoutStream: DataStream[OrderEvent] = env.readTextFile(resource.getPath).map(data => {
      val dataArr = data.split(",")
      OrderEvent(dataArr(0).toLong, dataArr(1), dataArr(2), dataArr(3).toLong)
    }).assignAscendingTimestamps(_.timestamp*1000)

    //定义orderPattern
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //应用到流

    val orderPatternStream: PatternStream[OrderEvent] = CEP.pattern(inoutStream.keyBy(_.orderId), orderPayPattern)

    //定义outputTag
    val outputTag = OutputTag[OrderResult]("time-out")

    val resStream: DataStream[OrderResult] = orderPatternStream
      .select(outputTag, new OrderTimeoutFun(), new OrderResultFun())

    val timeOutRes: DataStream[OrderResult] = resStream.getSideOutput(outputTag)

    timeOutRes.print("timeout")
    resStream.print("payed")

    env.execute()
  }

}

class OrderTimeoutFun() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    print(l)
    val timeoutOrder: OrderEvent = map.get("create").iterator().next()
    OrderResult(timeoutOrder.orderId.toLong, "timeout")
  }
}

class OrderResultFun() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {

    val payedOrder: OrderEvent = pattern.get("pay").iterator().next()

    OrderResult(payedOrder.orderId, "payed")
  }
}