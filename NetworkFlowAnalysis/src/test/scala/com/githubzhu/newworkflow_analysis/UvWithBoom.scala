package com.githubzhu.newworkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/3 19:13
 * @ModifiedBy:
 *
 */
object UvWithBoom {
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


    val uvCountStream :DataStream[UvCount]=  dataStream
      .filter(data=>data.behavior=="pv")
//      .map(data => {  ("pv", 1) })// map成二元组，用一个哑key来作为分组的key
      .map(data => {  ("pv", data.userId) })// map成二元组，用一个哑key来作为分组的key
      .keyBy(_._1)
      .timeWindow(Time.hours(1)) //统计每小时的pv值
      .trigger(new MyTrlgger())
      .process(new UvCountWithBloom())


    env.execute("pv-job")

  }
}

//自定义一个触发器
class MyTrlgger() extends Trigger[(String,Long),TimeWindow] {
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = {TriggerResult.FIRE_AND_PURGE}

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = {TriggerResult.CONTINUE}

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = {TriggerResult.CONTINUE}

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext) = {}
}

//自定义一个布隆过滤器，位图是在外部redis ，这里只保存位图的大小及hash 函数

class Bloom (size :Long)extends Serializable{
  //一般cap   是2 的整次方
  private val cap  = size

  //实现一个hash函数
  def hash(value:String ,seed :Int) :Long={
    var result = 0L
    for(i<- 0 to value.length-1) {
      //用每个字符ASCII 码值做叠加计算
      result = result * seed + value.charAt(i)
    }
    //返回 一个cap 范围 的hash值

    (cap - 1) & result
  }
}

//自定义处理逻辑
class  UvCountWithBloom() extends  ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow] {
  lazy val jedis = new Jedis("hadoop102",6379)
  // 需处理 1亿用户的去重，  定义布隆过滤器的大小为10亿，取2 的整数倍，2^30
  lazy val bloomfilter = new Bloom(1<<30)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //定义redis 中保存的位图的key 以当前的窗口end 为key，（windowEnd bitmap）
    val storeKey = context.window.getEnd.toString

    //把当前的uv 的count 值也保存到redis中保存成一张叫count 的hash表（windowEnd，uvcount）
    val countMapKey = "count"

    //初始化操作， 从redis 中count表中，查询当前窗口的uvcount 值
    var count = 0L
    if(jedis.hget(countMapKey,storeKey)!=null) {
      count = jedis.hget(countMapKey,storeKey).toLong
    }

    //去重操作，首先拿到userId
    val userId = elements.last._2.toString
    //用布隆过滤器的hash函数计算位图中的偏移量
    val offset = bloomfilter.hash(userId,61)

    //使用redis 命令，查询位图中对应位置是否为1
    val isExist : Boolean = jedis.getbit(storeKey,offset)
    if(!isExist) {
      //如果不存在userId ,对应位图的位置设置为1，count +1
      jedis.setbit(storeKey,offset,true)
      jedis.hset(countMapKey,storeKey,(count+1).toString)
    }


  }
}