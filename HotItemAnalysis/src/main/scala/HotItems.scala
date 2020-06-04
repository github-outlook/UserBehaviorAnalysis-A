import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/2 19:08
 * @ModifiedBy:
 *
 */
object HotItems {
  def main(args: Array[String]): Unit = {

    //创建一个流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //从文件读取数据
//    val inputStream: DataStream[String] = env.readTextFile("E:\\JavaWorkingSpace\\idea2019.2.3\\Flink\\UserBehaviorAnalysis-A\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")

    //从kafaka中读取数据，
        val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotitem",new SimpleStringSchema(),properties))

    inputStream.print()
    //转换成样例类，并分配时间戳和watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val dataArr: Array[String] = data.split(",")
        UserBehavior(dataArr(0).toLong, dataArr(1) toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)

      }).assignAscendingTimestamps(_.timestamp * 1000)

    //进行开窗聚合转换
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") //过滤出pv行为 用于热门度 的count统计
      .keyBy("itemId") // 按商品id进行分组
      .timeWindow(Time.hours(1), Time.minutes(5)) //定义滑动时间窗口
      .aggregate(new CountAgg(), new WindowResult())

    //对统计结果按照窗口进行分组，排序输出
    val resStream: DataStream[String] = aggStream
      .keyBy("windowEnd")
      .process(new TopNHotItems(5))

    resStream.print()
    env.execute()
  }
}


//自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数，将窗口信息包装进去
class WindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

//自定义keyedProcessFunction , 对每个窗口的count 统计值进行排序，并格式化字符串输出
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  //定义一个列表状态，用来保存当前窗口所有商品的count值
  private var itemViewCountListState: ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-liststate", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每来一条数据，就将它添加到ListState中
    itemViewCountListState.add(value)
    //注册一个windowEnd +1 d 定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }


  //当定时器触发时，当前窗口所有的商品的计数都到齐了，直接排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //遍历ListState 数据，放入ListBuffer方便排序
    val allItemViewCounts: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]()
    import scala.collection.JavaConversions._
    for (itemVew <- itemViewCountListState.get()) {
      allItemViewCounts += itemVew
    }

    //清空当前状态
    itemViewCountListState.clear()

    //按照count 大小进行排序  取topsize
    val sortedItemViewCounts: ListBuffer[ItemViewCount] = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //将排序数据包装成可视化String，便于打印

    val result: StringBuilder = new StringBuilder
    result.append("======================\n")
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

    //遍历排序数据结果   将每个ItemViewCount 的商品ID 和Count 值  ，及排名输出
    for (i <- sortedItemViewCounts.indices) {
      val currentViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i + 1).append(":")
        .append("商品ID= ").append(currentViewCount.itemId)
        .append("点击量= ").append(currentViewCount.count)
        .append("\n")
    }
    Thread.sleep(100)
    out.collect(result.toString())

  }
}


//定义样例类 ： 输入数据
case class UserBehavior(userId: Long, itemId: Long, catagoryId: Int, behavior: String, timestamp: Long)

//中间聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)