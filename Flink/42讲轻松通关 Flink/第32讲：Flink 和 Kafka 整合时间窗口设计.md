## 第32讲：Flink 和 Kafka 整合时间窗口设计

[TOC]

我们在第 31 课时中讲过，在计算 PV 和 UV 等指标前，用 Flink 将原始数据进行了清洗，清洗完毕的数据被发送到另外的 Kafka Topic 中，接下来我们只需要消费指定 Topic 的数据，然后就可以进行指标计算了。

### Flink 消费 Kafka 数据反序列化
上一课时定义了用户的行为信息的 Java 对象，我们现在需要消费新的 Kafka Topic 信息，并且把序列化的消息转化为用户的行为对象：

```
public class UserClick { 
    private String userId; 
    private Long timestamp; 
    private String action; 
    public String getUserId() { 
        return userId; 
    } 
    public void setUserId(String userId) { 
        this.userId = userId; 
    } 
    public Long getTimestamp() { 
        return timestamp; 
    } 
    public void setTimestamp(Long timestamp) { 
        this.timestamp = timestamp; 
    } 
    public String getAction() { 
        return action; 
    } 
    public void setAction(String action) { 
        this.action = action; 
    } 
    public UserClick(String userId, Long timestamp, String action) { 
        this.userId = userId; 
        this.timestamp = timestamp; 
        this.action = action; 
    } 
} 
enum UserAction{ 
    //点击 
    CLICK("CLICK"), 
    //购买 
    PURCHASE("PURCHASE"), 
    //其他 
    OTHER("OTHER"); 
    private String action; 
    UserAction(String action) { 
        this.action = action; 
    } 
} 
```

首先，我们需要新建自己的 Kafka Condumer，在第 12 和 24 课时中详细讲解了 Flink 消费 Kafak 消息的原理和实现。在计算 PV 和 UV 的业务场景中，我们选择使用消息中自带的事件时间作为时间特征，代码如下：

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); 
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); 
// 检查点配置,如果要用到状态后端，那么必须配置 
env.setStateBackend(new MemoryStateBackend(true)); 
Properties properties = new Properties(); 
properties.setProperty("bootstrap.servers", "127.0.0.1:9092"); 
properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "10"); 
FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("log_user_action", new SimpleStringSchema(), properties); 
//设置从最早的offset消费 
consumer.setStartFromEarliest(); 
DataStream<UserClick> dataStream = env 
        .addSource(consumer) 
        .name("log_user_action") 
        .map(message -> { 
            JSONObject record = JSON.parseObject(message); 
            return new UserClick( 
                    record.getString("user_id"), 
                    record.getLong("timestamp"), 
                    record.getString("action") 
            ); 
        }) 
        .returns(TypeInformation.of(UserClick.class)); 
```

在上面的代码中，我们消费第 31 课时中写入的新的 Kafka Topic，并且将读取到的数据反序列化为 UserClick 的 DataStream。

### 水印和窗口设计
我们得到 UserClick 的 DataStream 后，需要进行水印和窗口的设计，可在 DataStream 上调用 assignTimestampsAndWatermarks 方法。我们在第 8 和 25 课时中，详细讲解过 Flink 支持的时间戳提取器和水印发射器：

周期性水印：AssignerWithPeriodicWatermarks

特定事件触发水印：PunctuatedWatermark

由于我们的用户访问日志可能存在乱序，所以使用 BoundedOutOfOrdernessTimestampExtractor  来处理乱序消息和延迟时间，我们指定消息的乱序时间 30 秒，具体代码如下：

```
SingleOutputStreamOperator<UserClick> userClickSingleOutputStreamOperator = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserClick>(Time.seconds(30)) { 
    @Override 
    public long extractTimestamp(UserClick element) { 
        return element.getTimestamp(); 
    } 
}); 
```

### 窗口触发器
#### 窗口设置
到目前为止，我们已经通过读取 Kafka 中的数据，序列化为用户点击事件的 DataStream，并且完成了水印和时间戳的设计和开发。

接下来，按照业务需要，我们需要开窗并且进行一天内用户点击事件的 PV、UV 计算。在第 8 课时中讲解了 Flink 支持的窗口类型：

- 滚动窗口，窗口数据有固定的大小，窗口中的数据不会叠加；
- 滑动窗口，窗口数据有固定的大小，并且有生成间隔；
- 会话窗口，窗口数据没有固定的大小，根据用户传入的参数进行划分，窗口数据无叠加。

很明显，我们需要使用滚动窗口：TumblingProcessingTimeWindow，并且按照一般逻辑，需每天 0 点到 24 点进行一次计算和输出。

这里我们使用 Flink 提供的滚动窗口，在构造滚动窗口之前先来看一下滚动窗口的实现：

```
public class TumblingEventTimeWindows extends WindowAssigner<Object, TimeWindow> { 
   private static final long serialVersionUID = 1L; 
   private final long size; 
   private final long offset; 
   protected TumblingEventTimeWindows(long size, long offset) { 
      if (Math.abs(offset) >= size) { 
         throw new IllegalArgumentException("TumblingEventTimeWindows parameters must satisfy abs(offset) < size"); 
      } 
      this.size = size; 
      this.offset = offset; 
   } 
```

这里需要注意的是，很多人会认为，窗口时间的开始时间会是我们的代码启动时间。事实上，根据上面的源码可见，TumblingEventTimeWindows 在构造时，需要指定两个参数：**窗口的长度和窗口的 offset(默认为 0)**。

```
/** 
 * Creates a new {@code TumblingEventTimeWindows} {@link WindowAssigner} that assigns 
 * elements to time windows based on the element timestamp and offset. 
 * 
 * <p>For example, if you want window a stream by hour,but window begins at the 15th minutes 
 * of each hour, you can use {@code of(Time.hours(1),Time.minutes(15))},then you will get 
 * time windows start at 0:15:00,1:15:00,2:15:00,etc. 
 * 
 * <p>Rather than that,if you are living in somewhere which is not using UTC±00:00 time, 
 * such as China which is using UTC+08:00,and you want a time window with size of one day, 
 * and window begins at every 00:00:00 of local time,you may use {@code of(Time.days(1),Time.hours(-8))}. 
 * The parameter of offset is {@code Time.hours(-8))} since UTC+08:00 is 8 hours earlier than UTC time. 
 * 
 * @param size The size of the generated windows. 
 * @param offset The offset which window start would be shifted by. 
 * @return The time policy. 
 */ 
public static TumblingEventTimeWindows of(Time size, Time offset) { 
   return new TumblingEventTimeWindows(size.toMilliseconds(), offset.toMilliseconds()); 
} 
```

我们如何取得某一天的 0 点这个时间起始点呢？TumblingEventTimeWindows 的 of 方法中已经给了提示：

> window begins at every 00:00:00 of local time,you may use {@code of(Time.days(1),Time.hours(-8))}.

我们只需要通过 TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)) 就可以指定在中国的 0 点开始创建窗口，然后每天计算一次输出结果即可。

但是，在实际生产环境中，对于大窗口的计算，一般都会设置触发器，以一定的频率输出中间结果，而不是等到一天结束时仅仅触发一次。

这里我们就需要设置数据流的 Trigger 属性。

#### 触发器设置

窗口的计算是依赖触发器进行的，每种类型的窗口都有自己的触发器机制，如果用户没有指定，那么会使用默认的触发器。例如 TumblingEventTimeWindows 中自带的触发器如下：

```
@Override 
public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) { 
   return EventTimeTrigger.create(); 
} 
```

可以看到源码中包含了一个 DefaultTrigger：EventTimeTrigger，而 EventTimeTrigger 的实现如下：

```
public class EventTimeTrigger extends Trigger<Object, TimeWindow> { 
   private static final long serialVersionUID = 1L; 
   private EventTimeTrigger() {} 
   @Override 
   public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception { 
      if (window.maxTimestamp() <= ctx.getCurrentWatermark()) { 
         return TriggerResult.FIRE; 
      } else { 
         ctx.registerEventTimeTimer(window.maxTimestamp()); 
         return TriggerResult.CONTINUE; 
      } 
   } 
   @Override 
   public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) { 
      return time == window.maxTimestamp() ? 
         TriggerResult.FIRE : 
         TriggerResult.CONTINUE; 
   } 
   @Override 
   public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception { 
      return TriggerResult.CONTINUE; 
   } 
   @Override 
   public void clear(TimeWindow window, TriggerContext ctx) throws Exception { 
      ctx.deleteEventTimeTimer(window.maxTimestamp()); 
   } 
   @Override 
   public boolean canMerge() { 
      return true; 
   } 
   @Override 
   public void onMerge(TimeWindow window, 
         OnMergeContext ctx) { 
      long windowMaxTimestamp = window.maxTimestamp(); 
      if (windowMaxTimestamp > ctx.getCurrentWatermark()) { 
         ctx.registerEventTimeTimer(windowMaxTimestamp); 
      } 
   } 
   @Override 
   public String toString() { 
      return "EventTimeTrigger()"; 
   } 
   public static EventTimeTrigger create() { 
      return new EventTimeTrigger(); 
   } 
} 
```

我们可以看到，EventTimeTrigger 触发器的工作原理是 判断当前的水印是否超过了窗口的结束时间，如果超过则触发对窗口内数据的计算，否则不触发计算。
Flink 本身提供了不同种类的触发器供我们使用，如下图所示：

![image.png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgqCHl8n4MiAdhQPAACVaOaaD2Y990.png)

触发器的类图如上图所示，它们的实际含义如下：

- EventTimeTrigger：通过对比 Watermark 和窗口的 Endtime 确定是否触发窗口计算，如果 Watermark 大于 Window EndTime 则触发，否则不触发，窗口将继续等待。
- ProcessTimeTrigger：通过对比 ProcessTime 和窗口 EndTime 确定是否触发窗口，如果 ProcessTime 大于 EndTime 则触发计算，否则窗口继续等待。
- ContinuousEventTimeTrigger：根据间隔时间周期性触发窗口或者 Window 的结束时间小于当前 EndTime 触发窗口计算。
- ContinuousProcessingTimeTrigger：根据间隔时间周期性触发窗口或者 Window 的结束时间小于当前 ProcessTime 触发窗口计算。
- CountTrigger：根据接入数据量是否超过设定的阈值判断是否触发窗口计算。
- DeltaTrigger：根据接入数据计算出来的 Delta 指标是否超过指定的 Threshold 去判断是否触发窗口计算。
- PurgingTrigger：可以将任意触发器作为参数转换为 Purge 类型的触发器，计算完成后数据将被清理。

我们在这里可以选择使用：ContinuousProcessingTimeTrigger 来周期性的触发窗口阶段性计算。

实现代码如下：

```
dataStream     .windowAll(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8))) 
.trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(20))) 
```

我们使用 ContinuousProcessingTimeTrigger 每隔 20 秒触发计算，输出中间结果。

到目前为止，完成了除计算 PV 和 UV 的所有前置条件的开发，我们实现了：清洗原始数据、写入新的 Kafka Topic、消费新的 Topic 信息、自定义水印和时间戳、自定义窗口和触发器。

## 总结
这一课时我们学习了 Flink 消费 Kafka 数据计算 PV 和 UV 的水印和窗口设计，并且定义了窗口计算的触发器，完成了计算 PV 和 UV 前的所有准备工作。

