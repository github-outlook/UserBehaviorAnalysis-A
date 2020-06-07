package com.githubzhu.marketanalysis

import java.lang
import java.sql.Timestamp
import java.util.{Random, UUID}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/5 16:51
 * @ModifiedBy:
 *
 */
//输入样例类
case class MarketingUserBehavior(userId: String, channel: String, behavior: String, timestamp: Long)

//输出样例类
case class MarketingByChannelCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

//定义一个自定义模拟测试源   RichParallelSourceFunction           RichParallelSourceFunction
class SimulatedEventSource() extends RichParallelSourceFunction[MarketingUserBehavior] {

  //定义是否运行标志位
  var running = true

  //定义渠道和用户行为
  val channelSet: Seq[String] = Seq("app-store", "huawei-store", "wechat")
  val BehaviorSet: Seq[String] = Seq("click", "download", "install", "uninstall")
  val rand: Random = new Random()

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    //定义最大的数据数量 ，用于控制测试规模

    val maxcounts = Long.MaxValue
    var count = 0L

    // 无限循环 ，随机生成所有的数据
    while (running && count < maxcounts) {

      val id = UUID.randomUUID().toString
      val channel = channelSet(rand.nextInt(channelSet.size))
      val behavior = BehaviorSet(rand.nextInt(BehaviorSet.size))
      val ts = System.currentTimeMillis()

      //使用ctx 发出数据
      ctx.collect(MarketingUserBehavior(id, channel, behavior, ts))
      count += 1
      Thread.sleep(10L)
    }
  }

  override def cancel(): Unit = running = false
}

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从自定义数据源读取数据进行处理
    val dataStream: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource).assignAscendingTimestamps(_.timestamp)

    // 开窗统计，得到渠道的聚合结果

    val resuSteam: DataStream[MarketingByChannelCount] = dataStream
      .filter(data => data.behavior != "unstall")
      .keyBy(data => (data.channel, data.behavior))
      .timeWindow(Time.hours(1), Time.seconds(5))

      .process(new MarketingByChannelCountResult())
    resuSteam.print()

    env.execute("app marketing by channel job")

  }

}

class MarketingByChannelCountResult() extends ProcessWindowFunction[MarketingUserBehavior, MarketingByChannelCount, (String, String), TimeWindow] {

  override def process(key: (String, String), context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[MarketingByChannelCount]): Unit = {

    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    val behavior = key._2
    val channel = key._1
    val count = elements.size

    out.collect(MarketingByChannelCount(start, end, channel, behavior, count))
  }
}