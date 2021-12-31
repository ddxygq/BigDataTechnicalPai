**PV(访问量)**：即Page View, 即页面浏览量或点击量，用户每次刷新即被计算一次。

**UV(独立访客)**：即Unique Visitor,访问您网站的一台电脑客户端为一个访客。00:00-24:00内相同的客户端只被计算一次。

计算网站App的实时pv和uv，是很常见的统计需求，这里提供通用的计算方法，不同的业务需求只需要小改即可拿来即用。



## 需求

利用Flink实时统计，从0点到当前的pv、uv。



## 一、需求分析

从`Kafka`发送过来的数据含有：`时间戳`、`时间`、`维度`、`用户id`，需要从不同维度统计从0点到当前时间的`pv`和`uv`，第二天0点重新开始计数第二天的。



## 二、技术方案

- `Kafka`数据可能会有延迟乱序，这里引入`watermark`；
- 通过`keyBy`分流进不同的滚动`window`，每个窗口内计算`pv`、`uv`；
- 由于需要保存一天的状态，`process`里面使用ValueState保存`pv`、`uv`；
- 使用`BitMap`类型`ValueState`，占内存很小，引入支持`bitmap`的依赖；
- 保存状态需要设置`ttl`过期时间，第二天把第一天的过期，避免内存占用过大。



## 三、数据准备

这里假设是用户订单数据，数据格式如下：

```json
{"time":"2021-10-31 22:00:01","timestamp":"1635228001","product":"苹果手机","uid":255420}
{"time":"2021-10-31 22:00:02","timestamp":"1635228001","product":"MacBook Pro","uid":255421}
```

## 四、代码实现

整个工程代码截图如下（抹去了一些不方便公开的信息）：

![pvuv-project](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/2021/10/31/21:18:27-pvuv-project.png)

#### 1. 环境

`kafka`：1.0.0；

`Flink`：1.11.0；

#### 2. 发送测试数据

首先发送数据到`kafka`测试集群，`maven`依赖：

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>2.4.1</version>
</dependency>
```

发送代码：

```java
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import jodd.util.ThreadUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.io.*;

public class SendDataToKafka {

    @Test
    public void sendData() throws IOException {
        String inpath = "E:\\我的文件\\click.txt";
        String topic = "click_test";
        int cnt = 0;
        String line;
        InputStream inputStream = new FileInputStream(inpath);
        Reader reader = new InputStreamReader(inputStream);
        LineNumberReader lnr = new LineNumberReader(reader);
        while ((line = lnr.readLine()) != null) {
            // 这里的KafkaUtil是个生产者、消费者工具类，可以自行实现
            KafkaUtil.sendDataToKafka(topic, String.valueOf(cnt), line);
            cnt = cnt + 1;
            ThreadUtil.sleep(100);
        }
    }
}
```

#### 3. 主要程序

先定义个`pojo`：

```java
@NoArgsConstructor
@AllArgsConstructor
@Data
@ToString
public class UserClickModel {
    private String date;
    private String product;
    private int uid;
    private int pv;
    private int uv;
}
```



接着就是使用`Flink`消费kafka，指定`Watermark`，通过`KeyBy`分流，进入`滚动窗口`函数通过状态保存`pv`和`uv`。

```java
public class UserClickMain {

    private static final Map<String, String> config = Configuration.initConfig("commons.xml");

    public static void main(String[] args) throws Exception {

        // 初始化环境，配置相关属性
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        senv.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        senv.setStateBackend(new FsStateBackend("hdfs://bigdata/flink/checkpoints/userClick"));

        // 读取kafka
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", config.get("kafka-ipport"));
        kafkaProps.setProperty("group.id", config.get("kafka-groupid"));
        // kafkaProps.setProperty("auto.offset.reset", "earliest");

        // watrmark 允许数据延迟时间
        long maxOutOfOrderness = 5 * 1000L;
        SingleOutputStreamOperator<UserClickModel> dataStream = senv.addSource(
                new FlinkKafkaConsumer<>(
                        config.get("kafka-topic"),
                        new SimpleStringSchema(),
                        kafkaProps
                ))
                //设置watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(maxOutOfOrderness))
                        .withTimestampAssigner((element, recordTimestamp) -> {
                            // 时间戳须为毫秒
                            return Long.valueOf(JSON.parseObject(element).getString("timestamp")) * 1000;
                        })).map(new FCClickMapFunction()).returns(TypeInformation.of(new TypeHint<UserClickModel>() {
                }));

        // 按照 (date, product) 分组
        dataStream.keyBy(new KeySelector<UserClickModel, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(UserClickModel value) throws Exception {
                return Tuple2.of(value.getDate(), value.getProduct());
            }
        })
                // 一天为窗口，指定时间起点比时间戳时间早8个小时
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                // 10s触发一次计算，更新统计结果
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                // 计算pv uv
                .process(new MyProcessWindowFunctionBitMap())
                // 保存结果到mysql
                .addSink(new FCClickSinkFunction());

        senv.execute(UserClickMain.class.getSimpleName());
    }
}
```

代码都是一些常规代码，但是还是有几点需要注意的。

**注意**

1. 设置watermark，flink1.11中使用WatermarkStrategy，老的已经废弃了；
2. 我的数据里面时间戳是秒，需要乘以1000，flink提取时间字段，必须为`毫秒`；
3. `.window`只传入一个参数，表明是滚动窗口，`TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8))`这里指定了窗口的大小为一天，由于中国北京时间是`东8区`，比国际时间早8个小时，需要引入`offset`，可以自行进入该方法源码查看英文注释。

```
Rather than that,if you are living in somewhere which is not using UTC±00:00 time,
* such as China which is using UTC+08:00,and you want a time window with size of one day,
* and window begins at every 00:00:00 of local time,you may use {@code of(Time.days(1),Time.hours(-8))}.
* The parameter of offset is {@code Time.hours(-8))} since UTC+08:00 is 8 hours earlier than UTC time.
```

4. 一天大小的窗口，根据`watermark`机制一天触发计算一次，显然是不合理的，需要用`trigger`函数指定触发间隔为`10s`一次，这样我们的`pv`和`uv`就是`10s`更新一次结果。



#### 4. 关键代码，计算uv

由于这里`用户id`刚好是数字，可以使用`bitmap`去重，简单原理是：`把 user_id 作为 bit 的偏移量 offset，设置为 1 表示有访问，使用 1 MB的空间就可以存放 800 多万用户的一天访问计数情况`。

`redis`是自带`bit`数据结构的，不过为了尽量少依赖外部存储媒介，这里自己实现`bit`，引入相应`maven`依赖即可：

```xml
<dependency>
    <groupId>org.roaringbitmap</groupId>
    <artifactId>RoaringBitmap</artifactId>
    <version>0.8.0</version>
</dependency>
```

计算pv、uv的代码其实都是通用的，可以根据自己的实际业务情况快速修改的：

```java
public class MyProcessWindowFunctionBitMap extends ProcessWindowFunction<UserClickModel, UserClickModel, Tuple<String, String>, TimeWindow> {

    private transient ValueState<Integer> pvState;
    private transient ValueState<Roaring64NavigableMap> bitMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Integer> pvStateDescriptor = new ValueStateDescriptor<>("pv", Integer.class);
        ValueStateDescriptor<Roaring64NavigableMap> bitMapStateDescriptor = new ValueStateDescriptor("bitMap"
                , TypeInformation.of(new TypeHint<Roaring64NavigableMap>() {}));

        // 过期状态清除
        StateTtlConfig stateTtlConfig = StateTtlConfig
                .newBuilder(Time.days(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        // 开启ttl
        pvStateDescriptor.enableTimeToLive(stateTtlConfig);
        bitMapStateDescriptor.enableTimeToLive(stateTtlConfig);

        pvState = this.getRuntimeContext().getState(pvStateDescriptor);
        bitMapState = this.getRuntimeContext().getState(bitMapStateDescriptor);
    }

    @Override
    public void process(Tuple2<String, String> key, Context context, Iterable<UserClickModel> elements, Collector<UserClickModel> out) throws Exception {

        // 当前状态的pv uv
        Integer pv = pvState.value();
        Roaring64NavigableMap bitMap = bitMapState.value();
        if(bitMap == null){
            bitMap = new Roaring64NavigableMap();
            pv = 0;
        }

        Iterator<UserClickModel> iterator = elements.iterator();
        while (iterator.hasNext()){
            pv = pv + 1;
            int uid = iterator.next().getUid();
            //如果userId可以转成long
            bitMap.add(uid);
        }

        // 更新pv
        pvState.update(pv);

        UserClickModel UserClickModel = new UserClickModel();
        UserClickModel.setDate(key.f0);
        UserClickModel.setProduct(key.f1);
        UserClickModel.setPv(pv);
        UserClickModel.setUv(bitMap.getIntCardinality());

        out.collect(UserClickModel);
    }
}
```

**注意**

1. 由于计算`uv`第二天的时候，就不需要第一天数据了，要及时清理内存中`前一天`的状态，通过`ttl`机制过期；
2. 最终结果保存到mysql里面，如果数据结果分类聚合太多，要注意`mysql压力`，这块可以自行优化；

## 五、其它方法

除了使用`bitmap`去重外，还可以使用`Flink SQL`,编码更简洁，还可以借助外面的媒介`Redis`去重：

1. 基于 set
2. 基于 bit
3. 基于 HyperLogLog
4. 基于bloomfilter

具体思路是，计算`pv`、`uv`都塞入redis里面，然后再获取值保存统计结果，也是比较常用的。

**猜你喜欢**<br>
[HDFS的快照讲解](https://mp.weixin.qq.com/s/ooYIcHQ5V9x2fh3G7ZhCxg)<br>
[Hadoop 数据迁移用法详解](https://mp.weixin.qq.com/s/L8k0lO_ZbQy7G_46eshnCw)<br>
[Hbase修复工具Hbck](https://mp.weixin.qq.com/s/L2Nvi0HSCbG8pH-DK0cG1Q)<br>
[数仓建模分层理论](https://mp.weixin.qq.com/s/8rpDyo41Kr4r_2wp5hirVA)<br>
[一文搞懂Hive的数据存储与压缩](https://mp.weixin.qq.com/s/90MuP3utZx9BlgbwsfDsfw)<br>
[大数据组件重点学习这几个](https://mp.weixin.qq.com/s/4redHF0e7vCWFqv8t20Rjg)