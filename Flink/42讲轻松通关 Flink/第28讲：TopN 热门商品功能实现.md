## 第28讲：TopN 热门商品功能实现

[TOC]



本课时主要讲解 Flink 中的 TopN 功能的设计和实现。

TopN 在我们的业务场景中是十分常见的需求，比如电商场景中求热门商品的销售额、微博每天的热门话题 TopN、贴吧中每天发帖最多的贴吧排名等。TopN 可以进行分组排序，也可以按照需要全局排序，比如若要计算用户下单总金额的 Top 10 时，就需要进行全局排序，然而当我们计算每个城市的 Top10 时就需要将订单按照城市进行分组然后再进行计算。

下面我们就详细讲解 TopN 的设计和实现。

### 整体设计
我们下面使用订单数据进行讲解，整体的数据流向如下图所示：

![image (6).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgqCHl8VYd2AWtTGAADetnsxRT0968.png)

订单数据由业务系统产生并发送到 Kafka 中，我们的 Flink 代码会消费 Kafka 中的数据，并进行计算后写入 Redis，然后前端就可以通过读取 Redis 中的数据进行展示了。

订单设计
简化后的订单数据如下，主要包含：下单用户 ID、商品 ID、用户所在城市名称、订单金额和下单时间。

```
public class OrderDetail {
    private Long userId; //下单用户id
    private Long itemId; //商品id
    private String citeName;//用户所在城市
    private Double price;//订单金额
    private Long timeStamp;//下单时间
}
```

我们采用 Event-Time 来作为 Flink 程序的时间特征，并且设置 Checkpoint 时间周期为 60 秒。

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);
```

#### Kafka Consumer 实现

我们在第 24 课时“Flink 消费 Kafka 数据业务开发” 中详细讲解过 Kafka Consumer 的实现，在这里订阅 Kafka 消息作为数据源，设置从最早位点开始消费数据：

```
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
//从最早开始消费
consumer.setStartFromEarliest();
DataStream<String> stream = env
        .addSource(consumer);
```

#### 时间提取和水印设置
因为订单消息流可能存在乱序的问题，我们在这里设置允许乱序时间为 30 秒，并且设置周期性水印：

```
DataStream<OrderDetail> orderStream = stream.map(message -> JSON.parseObject(message, OrderDetail.class));
orderStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<OrderDetail>() {
    private Long currentTimeStamp = 0L;
    //设置允许乱序时间
    private Long maxOutOfOrderness = 5000L;
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTimeStamp - maxOutOfOrderness);
    }
    @Override
    public long extractTimestamp(OrderDetail element, long previousElementTimestamp) {
        return element.getTimeStamp();
    }
});
```

下单金额 TopN
我们在这里要求所有用户中下单金额最多的 Top 10 用户，这里就会用到 windowAll 函数：

```
DataStream<OrderDetail> reduce = dataStream
        .keyBy((KeySelector<OrderDetail, Object>) value -> value.getUserId())
        .window(SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(20)))
        .reduce(new ReduceFunction<OrderDetail>() {
            @Override
            public OrderDetail reduce(OrderDetail value1, OrderDetail value2) throws Exception {
                return new OrderDetail(
                        value1.getUserId(), value1.getItemId(), value1.getCiteName(), value1.getPrice() + value2.getPrice(), value1.getTimeStamp()
                );
            }
        });

//每20秒计算一次
SingleOutputStreamOperator<Tuple2<Double, OrderDetail>> process = reduce.windowAll(TumblingEventTimeWindows.of(Time.seconds(20)))
        .process(new ProcessAllWindowFunction<OrderDetail, Tuple2<Double, OrderDetail>, TimeWindow>() {
                     @Override
                     public void process(Context context, Iterable<OrderDetail> elements, Collector<Tuple2<Double, OrderDetail>> out) throws Exception {
                         TreeMap<Double, OrderDetail> treeMap = new TreeMap<Double, OrderDetail>(new Comparator<Double>() {
                             @Override
                             public int compare(Double x, Double y) {
                                 return (x < y) ? -1 : 1;
                             }
                         });
                         Iterator<OrderDetail> iterator = elements.iterator();
                         if (iterator.hasNext()) {
                             treeMap.put(iterator.next().getPrice(), iterator.next());
                             if (treeMap.size() > 10) {
                                 treeMap.pollLastEntry();
                             }
                         }
                         for (Map.Entry<Double, OrderDetail> entry : treeMap.entrySet()) {
                             out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                         }
                     }
                 }
        );
```


这段代码实现略显复杂，核心逻辑如下：
首先对输入流按照用户 ID 进行分组，并且自定义了一个滑动窗口，即 SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(20))，表示定义一个总时间长度为 600 秒，每 20 秒向后滑动一次的滑动窗口。

经过上面的处理后，我们的订单数据会按照用户维度每隔 20 秒进行一次计算，并且通过 windowAll 函数将所有的数据汇聚到一个窗口。

这里需要注意，windowAll 是一个并发度为 1 的特殊操作，也就是所有元素都会进入到一个窗口内进行计算。

那么我们是如何取得 Top 10 的呢？

在这里，我们定义了一个 TreeMap，TreeMap 存储 K-V 键值对，通过红黑树（R-B tree）实现，红黑树结构天然支持排序，默认情况下通过 Key 值的自然顺序进行排序。我们设置的 TreeMap 大小是 10，如果新的数据到来后，TreeMap 的数据已经到达 10个，那么就会进行比较，将较小的删除。

#### 写入 Redis
我们在第 27 课时中讲了 Flink Redis Sink 的实现，把结果存入 Redis 中，结构为 HASH：

```
FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
process.addSink(new RedisSink<>(conf, new RedisMapper<Tuple2<Double, OrderDetail>>() {
    private final String TOPN_PREFIX = "TOPN:";
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET,TOPN_PREFIX);
    }
    @Override
    public String getKeyFromData(Tuple2<Double, OrderDetail> data) {
        return String.valueOf(data.f0);
    }
    @Override
    public String getValueFromData(Tuple2<Double, OrderDetail> data) {
        return String.valueOf(data.f1.toString());
    }
}));
```

我们的 RedisSink 通过新建 RedisMapper 来实现，覆写了其中的 getCommandDescription、getKeyFromData、getValueFromData 三个方法。分别用来设定存储数据的格式，Redis 中 Key 的实现和 Value 的实现。

### 总结

这一课时我们详细讲解了 Flink 中 TopN 功能的实现，用到了之前课时中学习到的 Kafka Consumer、时间戳和水印设置，最后使用 TreeMap 存储计算的结果，并详细讲解和实现了 RedisSink。在一般的大屏业务情况下，大家都可以按照这个思路进行设计和实现。

