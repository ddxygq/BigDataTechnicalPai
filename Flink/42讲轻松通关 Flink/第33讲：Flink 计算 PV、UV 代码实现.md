## Flink 计算 PV、UV 代码实现

[TOC]

上一课时我们学习了 Flink 消费 Kafka 数据计算 PV 和 UV 的水印和窗口设计，并且定义了窗口计算的触发器，完成了计算 PV 和 UV 前的所有准备工作。

接下来就需要计算 PV 和 UV 了。在当前业务场景下，根据 userId 进行统计，PV 需要对 userId 进行统计，而 UV 则需要对 userId 进行去重统计。

下面我们使用不同的方法来统计 PV 和 UV。

### 单窗口内存统计

这种方法需要把一天内所有的数据进行缓存，然后在内存中遍历接收的数据，进行 PV 和 UV 的叠加统计。

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); 
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); 
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
SingleOutputStreamOperator<UserClick> userClickSingleOutputStreamOperator = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserClick>(Time.seconds(30)) { 
    @Override 
    public long extractTimestamp(UserClick element) { 
        return element.getTimestamp(); 
    } 
}); 
userClickSingleOutputStreamOperator 
        .windowAll(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8))) 
        .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(20))) 
...
```

在上一课时中我们已经定义了全局的窗口，并且自定义触发器，每 20 秒触发一次计算输出中间结果。

我们在后面可以继续调用 process 方法，自定义 ProcessFunction 如下：

```
public class MyProcessFunction extends ProcessAllWindowFunction<UserClick,Tuple2<String,Integer>,TimeWindow> { 
    @Override 
    public void process(Context context, Iterable<UserClick> elements, Collector<Tuple2<String, Integer>> out) throws Exception { 
        HashSet<String> uv = Sets.newHashSet(); 
        Integer pv = 0; 
        Iterator<UserClick> iterator = elements.iterator(); 
        while (iterator.hasNext()){ 
            String userId = iterator.next().getUserId(); 
            uv.add(userId); 
            pv = pv + 1; 
        } 
        out.collect(Tuple2.of("pv",pv)); 
        out.collect(Tuple2.of("uv",uv.size())); 
    } 
}
```

我们自定义的 ProcessFunction 继承了 ProcessAllWindowFunction 并且覆写了 process 方法。在 process 方法中，新建了一个 HashSet，利用 HashSet 对 userId 去重，即是我们需要的 UV。PV 初始化为 0 并且每来一条数据则加1，最后得到的就是 PV。

TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)) 方法每天从 0 点开始计算并且每天都会清空数据。

这种方法代码简单清晰，但是有很严重的内存占用问题。如果我们的数据量很大，那么所定义的 TumblingProcessingTimeWindows 窗口会缓存一整天的数据，内存消耗非常大。

### 分组窗口 + 过期数据剔除

为了减少窗口内缓存的数据量，我们可以根据用户的访问时间戳所在天进行分组，然后将数据分散在各个窗口内进行计算，接着在 State 中进行汇总。

首先，我们把 DataStream 按照用户的访问时间所在天进行分组：

```
userClickSingleOutputStreamOperator 
        .keyBy(new KeySelector<UserClick, String>() { 
            @Override 
            public String getKey(UserClick value) throws Exception { 
                return DateUtil.timeStampToDate(value.getTimestamp()); 
            } 
        }) 
        .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8))) 
        .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(20))) 
        .evictor(TimeEvictor.of(Time.seconds(0), true)) 
        ...
```

然后根据用户的访问时间所在天进行分组并且调用了 evictor 来剔除已经计算过的数据。

其中的 DateUtil 是获取时间戳的年月日：

```
public class DateUtil {
    public static String timeStampToDate(Long timestamp){
        ThreadLocal<SimpleDateFormat> threadLocal
                = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        String format = threadLocal.get().format(new Date(timestamp));
        return format.substring(0,10);
    }
}
```

接下来看看 Flink 中的剔除器原理和使用方法。

剔除器
Flink 中的剔除器可以在 Window Function 执行前或后使用，用来从 Window 中剔除元素。目前 Flink 支持了三种类型的剔除器，具体如下。

CountEvictor：数量剔除器。在 Window 中保留指定数量的元素，并从窗口头部开始丢弃其余元素。

DeltaEvictor：阈值剔除器。计算 Window 中最后一个元素与其余每个元素之间的增量，丢弃增量大于或等于阈值的元素。

TimeEvictor：时间剔除器。保留 Window 中最近一段时间内的元素，并丢弃其余元素。

剔除器源码
我们在这里使用了 TimeEvictor，来看看该剔除器的源码实现：

```
private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) { 
   if (!hasTimestamp(elements)) { 
      return; 
   } 
   long currentTime = getMaxTimestamp(elements); 
   long evictCutoff = currentTime - windowSize; 
   for (Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext(); ) { 
      TimestampedValue<Object> record = iterator.next(); 
      if (record.getTimestamp() <= evictCutoff) { 
         iterator.remove(); 
      } 
   } 
} 
...
```

可以看到，该剔除器首先会找到窗口中元素的最大时间戳，并且找到当前窗口的截断点：最大的时间戳减去要保留的时间段；然后遍历窗口中每一个元素，如果当前元素的时间戳小于等于截断点，则剔除该元素。

所以，我们定义的 TimeEvictor.of(Time.seconds(0), true) 则会剔除每条计算过的元素。

接下来我们实现自己的 ProcessFunction：

```
public class MyProcessWindowFunction extends ProcessWindowFunction<UserClick,Tuple3<String,String, Integer>,String,TimeWindow>{ 
    private transient MapState<String, String> uvState; 
    private transient ValueState<Integer> pvState; 
    @Override 
    public void open(Configuration parameters) throws Exception { 
        super.open(parameters); 
        uvState = this.getRuntimeContext().getMapState(new MapStateDescriptor<>("uv", String.class, String.class)); 
        pvState = this.getRuntimeContext().getState(new ValueStateDescriptor<Integer>("pv", Integer.class)); 
    } 
    @Override 
    public void process(String s, Context context, Iterable<UserClick> elements, Collector<Tuple3<String, String, Integer>> out) throws Exception { 
        Integer pv = 0; 
        Iterator<UserClick> iterator = elements.iterator(); 
        while (iterator.hasNext()){ 
            pv = pv + 1; 
            String userId = iterator.next().getUserId(); 
            uvState.put(userId,null); 
        } 
        pvState.update(pvState.value() + pv); 
        Integer uv = 0; 
        Iterator<String> uvIterator = uvState.keys().iterator(); 
        while (uvIterator.hasNext()){ 
            String next = uvIterator.next(); 
            uv = uv + 1; 
        } 
        Integer value = pvState.value(); 
        if(null == value){ 
            pvState.update(pv); 
        }else { 
            pvState.update(value + pv); 
        } 
        out.collect(Tuple3.of(s,"uv",uv)); 
        out.collect(Tuple3.of(s,"pv",pvState.value())); 
    } 
}
```

在 State 中进行 PV 和 UV 的迭代计算，并且将计算过的数据剔除出窗口。保证窗口中的数据不会占用太多内存。

我们在主程序中可以直接使用：

```
userClickSingleOutputStreamOperator 
        .keyBy(new KeySelector<UserClick, String>() { 
            @Override 
            public String getKey(UserClick value) throws Exception { 
                return value.getUserId(); 
            } 
        }) 
        .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8))) 
        .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(20))) 
        .evictor(TimeEvictor.of(Time.seconds(0), true)) 
        .process(new MyProcessWindowFunction());
```

### 使用 BitMap / 布隆过滤器
我们在计算 UV 时需要进行全局去重，在第 20 课时“Flink 高级应用之海量数据高效去重”中介绍过大数量下的去重方法，这里我们就可以使用 BitMap 或者布隆过滤器进行去重。

假如用户的 ID 可以转化为 Long 型，可以使用 BitMap 进行去重计算 UV：

```
public class MyProcessWindowFunctionBitMap extends ProcessWindowFunction<UserClick,Tuple3<String,String, Integer>,String,TimeWindow>{ 
    private transient ValueState<Integer> uvState; 
    private transient ValueState<Integer> pvState; 
    private transient ValueState<Roaring64NavigableMap> bitMapState; 

    @Override 
    public void open(Configuration parameters) throws Exception { 
        super.open(parameters); 
        uvState = this.getRuntimeContext().getState(new ValueStateDescriptor<Integer>("uv", Integer.class)); 
        pvState = this.getRuntimeContext().getState(new ValueStateDescriptor<Integer>("pv", Integer.class)); 
        bitMapState = this.getRuntimeContext().getState(new ValueStateDescriptor("bitMap", TypeInformation.of(new TypeHint<Roaring64NavigableMap>() { 
        }))); 
    } 
    @Override 
    public void process(String s, Context context, Iterable<UserClick> elements, Collector<Tuple3<String, String, Integer>> out) throws Exception { 
        Integer uv = uvState.value(); 
        Integer pv = pvState.value(); 
        Roaring64NavigableMap bitMap = bitMapState.value(); 
        if(bitMap == null){ 
            bitMap = new Roaring64NavigableMap(); 
            uv = 0; 
            pv = 0; 
        } 
        Iterator<UserClick> iterator = elements.iterator(); 
        while (iterator.hasNext()){ 
            pv = pv + 1; 
            String userId = iterator.next().getUserId(); 
            //如果userId可以转成long 
            bitMap.add(Long.valueOf(userId)); 
        } 
        out.collect(Tuple3.of(s,"uv",bitMap.getIntCardinality())); 
        out.collect(Tuple3.of(s,"pv",pv)); 

    } 
}
```

上面定义了一个 Roaring64NavigableMap 用来存储用户 ID，最后只需要调用 bitMap.getIntCardinality() 即可求得去重后的 UV 值。

当然了，如果业务中的用户 ID 是字符串类型，不能被转化为 Long 型，那么你可以使用布隆过滤器进行去重。

## 总结

这一课时我们使用了三种方法进行了 PV 和 UV 的计算，分别是全窗口内存统计、使用分组和过期数据剔除、使用 BitMap / 布隆过滤器，在实际业务中我们可以根据业务场景灵活使用。