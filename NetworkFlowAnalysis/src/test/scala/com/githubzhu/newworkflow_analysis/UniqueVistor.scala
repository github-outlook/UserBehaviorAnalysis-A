package com.githubzhu.newworkflow_analysis

import akka.stream.stage.TimerGraphStageLogicWithLogging
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/3 19:13
 * @ModifiedBy:
 *
 */

case class UvCount(windowEnd:Long,count:Long)

object UniqueVistor {
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

    .timeWindowAll(Time.hours(1)) //统计每小时的pv值
    .apply(new MyUvCountResult())


  env.execute("pv-job")


}
//自定义全窗口函数
class MyUvCountResult() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //用一个集合来保存所有的userId ,实现自动去重
    var idSet = Set[Long]()
    for(ub<-input) {
      idSet += ub.userId
    }
    //包装好样例类输出
    out.collect(UvCount(window.getEnd,idSet.size))
  }
}