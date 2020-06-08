package order_pay_detect

import com.sun.corba.se.impl.resolver.ORBInitRefResolverImpl
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/8 16:47
 * @ModifiedBy:
 *
 */
//定义到账事件样例类 订单事件样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

case class OrderResult(orderId: Long, status: String)

object TxMatch {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderResource = getClass.getResource("/OrderLog.csv")
    val orderInputStream: KeyedStream[OrderEvent, String] = env.readTextFile(orderResource.getPath).map(data => {
      val dataArr = data.split(",")
      OrderEvent(dataArr(0).toLong, dataArr(1), dataArr(2), dataArr(3).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000).filter(_.txId != "").keyBy(_.txId)

    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptInputStream: KeyedStream[ReceiptEvent, String] = env.readTextFile(receiptResource.getPath).map(data => {
      val dataArr = data.split(",")
      ReceiptEvent(dataArr(0), dataArr(1), dataArr(2).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000).keyBy(_.txId)

    //2. 链接两条流，做分别计算
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderInputStream.connect(receiptInputStream)
      .process(new TxPayMatchDetect())

    //定义outputTag
    val unmatchPays = OutputTag[OrderEvent]("unmatched-pays")
    val unmatchReceipts = OutputTag[ReceiptEvent]("unmatched-receipts")

    resultStream.print("matched")
    resultStream.getSideOutput(unmatchPays).print("unmatched-pays")
    resultStream.getSideOutput(unmatchReceipts).print("unmatched-receipts")

    env.execute("tx match job")
  }
}

//自定义CoProcessFunction 用状态保存另一条流已经来的数据
class TxPayMatchDetect() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  //定义状态，用来保存已经来到的 pay事件和receipt 事件
  lazy val payEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-event", classOf[OrderEvent]))
  lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-event", classOf[ReceiptEvent]))

  //订单流里 的数据每来一个  调用一次processElement
  override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //订单支付事件来了，要考察是否已经receipt
    val receipt = receiptEventState.value()

    if (receipt != null) { //如果来过，正常匹配输出到主流
      out.collect((pay, receipt))
      //清空状态
      payEventState.clear()
      receiptEventState.clear()
    } else {
      //如果没来，注册定时器等待
      ctx.timerService().registerEventTimeTimer(pay.timestamp * 1000L + 5000L)
      //更新状态
      payEventState.update(pay)
    }


  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //到账事件来了，要考虑当前是否已经来过pay
    val pay = payEventState.value()
    if (pay != null) { //如果来过，正常输出到测输出流
      out.collect((pay, receipt))
      //清空状态
      payEventState.clear()
      receiptEventState.clear()
    } else {
      //2.如果receipt还没来，注册定时器等待
      ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L)
    //更新状态
    receiptEventState.update(receipt)
    }
  }

  //定时器触发，需要判断状态中的值
  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //判断两个状态，那个不为空，那么就是另一个没来
    if (payEventState.value() != null) {
      ctx.output(new OutputTag[OrderEvent]("unmatched-pays"), payEventState.value())
    }
    if (receiptEventState.value() != null) {
      ctx.output(new OutputTag[ReceiptEvent]("unmatched-receipts"), receiptEventState.value())
    }

    receiptEventState.clear()
    payEventState.clear()
  }
}
