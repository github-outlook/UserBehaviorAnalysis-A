package com.githubzhu.newworkflow_analysis

import java.lang
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/3 19:13
 * @ModifiedBy:
 *
 */

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class PageViewCount(url: String, windowENd: Long, count: Long)

object NetWorkFlow {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("E:\\JavaWorkingSpace\\idea2019.2.3\\Flink\\UserBehaviorAnalysis-A\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")

    val dataStream = inputStream
      .map(data => {
        val dataArr = data.split(" ")
        //将时间String转换成Long的时间戳

        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = sdf.parse(dataArr(3)).getTime
        ApacheLogEvent(dataArr(0), dataArr(1), timestamp, dataArr(5), dataArr(6))

      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      override def extractTimestamp(element: ApacheLogEvent) = element.eventTime
    })

    val aggStream = dataStream
      .filter(_.method == "GET")
      /*

        .filter(data=>{
        val pattern = "^((?!\\.(css|js)$).)*$".r(pattern findFirstIn data.url).noempty
        })
       */
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late")) //允许一分钟迟到
      .aggregate(new PageCountAgg(), new PageCountWindowResult())

    val resStream = aggStream
      .keyBy(_.windowENd)
      .process(new TopNHotpages(3))

    dataStream.print("data")
    aggStream.print("agg")
    resStream.print("result")
    resStream.print(" Res")
    aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")

    env.execute("hot page job")

  }
}

//自定义预聚合函数
class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator() = 0L

  override def add(value: ApacheLogEvent, accumulator: Long) = accumulator + 1

  override def getResult(accumulator: Long) = accumulator

  override def merge(a: Long, b: Long) = a + b
}

//自定义窗口函数
class PageCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {

  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {

    out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
  }
}


class TopNHotpages(topSize: Long) extends KeyedProcessFunction[Long, PageViewCount, String] {

  //  lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageview-count", classOf[PageViewCount]))

  // 改进：定义MapState，用来保存当前窗口所有page的count值，有更新操作时直接put
  lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageview-count", classOf[String], classOf[Long]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]) = {
    //    pageViewCountListState.add(value)

    pageViewCountMapState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowENd + 1)

    //设置一分钟的timer 用于窗口关闭。清空数据
    ctx
      .timerService().registerEventTimeTimer(value.windowENd+60*1000)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]) :Unit = {

    if(timestamp == ctx.getCurrentKey + 60*1000) {
      pageViewCountMapState.clear()
      return
    }

    val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()

    //    val iter = pageViewCountListState.get().iterator()
    //    while (iter.hasNext) {
    //      allPageViewCounts += iter.next()
    //    }

    val iter = pageViewCountMapState.entries().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      allPageViewCounts += PageViewCount(entry.getKey, timestamp - 1, entry.getValue)

    }

//    pageViewCountListState.clear()
    val sortedPageViewCoounts = allPageViewCounts.sortWith(_.count > _.count).take(topSize.toInt)

    val result: StringBuilder = new StringBuilder
    result.append("==========================================\n")
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedPageViewCoounts.indices) {
      val currentViewCount = sortedPageViewCoounts(i)
      result.append("NO").append(i + 1).append(":")
        .append("页面URL= ").append(currentViewCount.url)
        .append("页面访问量=").append(currentViewCount.count)
        .append("\n")
    }

    //控制页面输出频率
    Thread.sleep(10)
    out.collect(result.toString())

  }
}