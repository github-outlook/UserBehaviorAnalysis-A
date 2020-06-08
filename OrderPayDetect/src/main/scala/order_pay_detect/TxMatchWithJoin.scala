package order_pay_detect

import order_pay_detect.TxMatch.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/8 16:47
 * @ModifiedBy:
 *
 */
object TxMatchWithJoin {
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

    //join
    val resStream: DataStream[(OrderEvent,ReceiptEvent)] = orderInputStream
      .intervalJoin(receiptInputStream)
      .between(Time.seconds(-3), Time.seconds(5))
      .process(new TxPayMatchDetectByJoin())

    resStream.print()
    env.execute()
  }
}

class TxPayMatchDetectByJoin() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)] {
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    out.collect((left,right))
  }
}