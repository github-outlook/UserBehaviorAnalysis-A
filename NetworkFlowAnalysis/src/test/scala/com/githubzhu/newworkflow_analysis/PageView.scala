package com.githubzhu.newworkflow_analysis

import org.apache.flink.api.common.functions.{AggregateFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/3 19:13
 * @ModifiedBy:
 *
 */

case class UserBehavior(userId: Long, itemId: Long, catagoryId: Int, behavior: String, timestamp: Long)

case class PvCount(windowEnd:Long,count:Long)
object PageView {

  def main(args: Array[String]): Unit = {

    //创建一个流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)

    //从文件读取数据
        val inputStream: DataStream[String] = env.readTextFile("E:\\JavaWorkingSpace\\idea2019.2.3\\Flink\\UserBehaviorAnalysis-A\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //转换成样例类，并分配时间戳和watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val dataArr: Array[String] = data.split(",")
        UserBehavior(dataArr(0).toLong, dataArr(1) toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)

      }).assignAscendingTimestamps(_.timestamp * 1000)


   val pvCountStream=  dataStream
      .filter(data=>data.behavior=="pv")
//      .map(data => {  ("pv", 1) })// map成二元组，用一个哑key来作为分组的key
      .map(new MyMapper())
      .keyBy(_._1)
      .timeWindow(Time.hours(1)) //统计每小时的pv值
      .aggregate(new PvCountAgg(), new PvCountWindowResult())

    pvCountStream.print("<>")

    //被把每个key 对应的pv count 值合并

    val totalPvCountStream = pvCountStream
      .keyBy(_.windowEnd)
      .process(new TotalPvCount())
      .print()

    env.execute("pv-job")

  }

}

//自定义预聚合函数
class PvCountAgg() extends  AggregateFunction[(String,Long),Long,Long]{
  override def createAccumulator() = 0

  override def add(value: (String, Long), accumulator: Long) = accumulator+1

  override def getResult(accumulator: Long) = accumulator

  override def merge(a: Long, b: Long) = a+b
}

class PvCountWindowResult() extends WindowFunction[Long,PvCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd,input.head))
  }
}

class TotalPvCount() extends KeyedProcessFunction[Long,PvCount,PvCount]{
  //定义一个状态，用来保存当前已有key 的count 值总计

  lazy val totalCountState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-counrt",classOf[Long]))
  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]) = {
    val currentTotalCount = totalCountState.value()
    totalCountState.update(currentTotalCount+value.count)

    //注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]) = {
    //定时器触发，直接输出当前的totalcount
    out.collect(PvCount(ctx.getCurrentKey,totalCountState.value()))
    //清空状态
    totalCountState.clear()

  }
}

//实现自定义Mapper
class MyMapper() extends  MapFunction[UserBehavior,(String,Long)] {
  override def map(value: UserBehavior) = {
    (Random.nextString(4),1)
  }
}

//