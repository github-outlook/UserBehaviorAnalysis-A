package order_pay_detect

import com.githubzhu.order_pay_detect.OrderPayDetect.getClass
import com.githubzhu.order_pay_detect.{OrderEvent, OrderResult, OrderResultFun, OrderTimeoutFun}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/8 7:39
 * @ModifiedBy:
 *
 */

case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

case class OrderResult(orderId: Long, status: String)

object OrderPayDetectInProcessFun {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/OrderLog.csv")
    val inputStream: DataStream[OrderEvent] = env.readTextFile(resource.getPath).map(data => {
      val dataArr = data.split(",")
      OrderEvent(dataArr(0).toLong, dataArr(1), dataArr(2), dataArr(3).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)

    val resStream = inputStream
      .keyBy(_.orderId)
      .process(new OrderStateFun())

    //定义outputTag
    val outputTag = OutputTag[OrderResult]("time-out")

    val timeOutRes: DataStream[OrderResult] = resStream.getSideOutput(outputTag)

    timeOutRes.print("time-out")
    resStream.print("payed")

    env.execute()

  }
}

//实现自定义KeyedProcessFunction 按照当前数据类型，及之前的状态，判断要做的操作
class OrderStateFun() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
  //定义状态，用来保存之前的create 和pay事件 是否已经来过
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))
  lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))

  //定义状态，保存定时器时间戳
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  //定义侧输出流标签，用于输出定时器时间戳
  val orderTimeoutOutputtag = new OutputTag[OrderResult]("time-out")


  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

    //先拿到当前状态
    val isPayed = isPayedState.value()
    val isCreated = isCreatedState.value()
    val timerTs = timerTsState.value()

    //判断当前的事件类型及之前的状态 不同的组合  有不同的处理流程
    //情况1 来的是create ，判断是否pay过

    if (value.eventType == "create") {
      //1.1 如果已经pay 过，匹配成功，输出到主流
      if (isPayed) {
        out.collect(OrderResult(value.orderId, "payed"))
        //结果已经输出，清空状态和定时器
        isPayedState.clear()
        isCreatedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }
      //1.2 如果没pay，注册定时器，等待
      else {
        val ts = value.timestamp * 1000L + 15 * 60 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        //更新状态和
        isCreatedState.update(true)
        timerTsState.update(ts)
      }
    }
    //情况2：来的是pay ，继续判断是否craeate
    else if (value.eventType == "pay") {
      //2.1 如果已经 pay ，判断时间差是否超过15分钟
      if (isCreated) {
        //2.1.1  如果没有超时，主流输出结果
        if (value.timestamp * 1000 <= timerTs) {
          out.collect(OrderResult(value.orderId, "payed"))
        }
        //如果已经超时 ，侧输出流输出少时结果
        else {
          ctx.output(orderTimeoutOutputtag, OrderResult(value.orderId, "payed but already timeout"))
        }
        //已经输出结果了 清空状态和定时器
        isPayedState.clear()
        isCreatedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }
      //情况2.2 ： 如果没有create ，可能数据丢失，也可能乱序，需要等待create
      else {
        //注册一个当前pay事件时间戳的定时器
        ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L)
        //更新状态
        isPayedState.update(true)
        timerTsState.update(value.timestamp * 1000L)
      }
    }
  }

  //定时器触发，肯定有一个事件没等到，输出一个异常信息
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {

    //如果isPayed 为true ，则 pay先到，没等到pay
    if (isPayedState.value())
      ctx.output(orderTimeoutOutputtag, OrderResult(ctx.getCurrentKey, "payed but not created"))
    else
      ctx.output(orderTimeoutOutputtag, OrderResult(ctx.getCurrentKey, "timeout"))

    //清空状态
    isPayedState.clear()
    isCreatedState.clear()
    timerTsState.clear()
  }
}
