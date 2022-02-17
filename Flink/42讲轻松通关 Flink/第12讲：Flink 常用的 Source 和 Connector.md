####  Flink系列文章

1. [第01讲：Flink 的应用场景和架构模型](https://www.ikeguang.com/?p=1976)
2. [第02讲：Flink 入门程序 WordCount 和 SQL 实现](https://www.ikeguang.com/?p=1977)
3. [第03讲：Flink 的编程模型与其他框架比较](https://www.ikeguang.com/?p=1978)
4. [第04讲：Flink 常用的 DataSet 和 DataStream API](https://www.ikeguang.com/?p=1982)
5. [第05讲：Flink SQL & Table 编程和案例](https://www.ikeguang.com/?p=1983)
6. [第06讲：Flink 集群安装部署和 HA 配置](https://www.ikeguang.com/?p=1985)
7. [第07讲：Flink 常见核心概念分析](https://www.ikeguang.com/?p=1986)
8. [第08讲：Flink 窗口、时间和水印](https://www.ikeguang.com/?p=1987)
9. [第09讲：Flink 状态与容错](https://www.ikeguang.com/?p=1988)
10. [第10讲：Flink Side OutPut 分流](https://www.ikeguang.com/?p=1991)
11. [第11讲：Flink CEP 复杂事件处理](https://www.ikeguang.com/?p=1992)
12. [第12讲：Flink 常用的 Source 和 Connector](https://www.ikeguang.com/?p=1993)
13. [第13讲：如何实现生产环境中的 Flink 高可用配置](https://www.ikeguang.com/?p=1994)
14. [第14讲：Flink Exactly-once 实现原理解析](https://www.ikeguang.com/?p=1995)
15. [第15讲：如何排查生产环境中的反压问题](https://www.ikeguang.com/?p=1998)
16. [第16讲：如何处理Flink生产环境中的数据倾斜问题](https://www.ikeguang.com/?p=1999)
17. [第17讲：生产环境中的并行度和资源设置](https://www.ikeguang.com/?p=2000)

本课时我们主要介绍 Flink 中支持的 Source 和常用的 Connector。

Flink 作为实时计算领域强大的计算能力，以及与其他系统进行对接的能力都非常强大。Flink 自身实现了多种 Source 和 Connector 方法，并且还提供了多种与第三方系统进行对接的 Connector。

我们可以把这些 Source、Connector 分成以下几个大类。

### 预定义和自定义 Source
在前面的第 04 课时“Flink 常用的 DataSet 和 DataStream API”中提到过几种 Flink 已经实现的新建 DataStream 方法。

#### 基于文件
我们在本地环境进行测试时可以方便地从本地文件读取数据：

```
readTextFile(path)
readFile(fileInputFormat, path)
```

可以直接在 ExecutionEnvironment 和 StreamExecutionEnvironment 类中找到 Flink 支持的读取本地文件的方法，如下图所示：

![image.png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgqCHl7LZOmARiTtAAUjtOQOdFM469.png)



![image (1).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqc1F7LZPCATHI4AAWm1YuLPzc592.png)



```
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
// read text file from local files system
DataSet<String> localLines = env.readTextFile("file:///path/to/my/textfile");
// read text file from an HDFS running at nnHost:nnPort
DataSet<String> hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile");
// read a CSV file with three fields
DataSet<Tuple3<Integer, String, Double>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
	                       .types(Integer.class, String.class, Double.class);
// read a CSV file with five fields, taking only two of them
DataSet<Tuple2<String, Double>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
                               .includeFields("10010")  // take the first and the fourth field
	                       .types(String.class, Double.class);
// read a CSV file with three fields into a POJO (Person.class) with corresponding fields
DataSet<Person>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
                         .pojoType(Person.class, "name", "age", "zipcode");
```

#### 基于 Collections

我们也可以基于内存中的集合、对象等创建自己的 Source。一般用来进行本地调试或者验证。

例如：

```
fromCollection(Collection)
fromElements(T ...)
```

我们也可以在源码中看到 Flink 支持的方法，如下图所示：

![image (2).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqc1F7LZQaAcf12AARuVRNchzI825.png)

```
DataSet<String> text = env.fromElements(
      "Flink Spark Storm",
      "Flink Flink Flink",
      "Spark Spark Spark",
      "Storm Storm Storm"
);
List data = new ArrayList<Tuple3<Integer,Integer,Integer>>();
data.add(new Tuple3<>(0,1,0));
data.add(new Tuple3<>(0,1,1));
data.add(new Tuple3<>(0,2,2));
data.add(new Tuple3<>(0,1,3));
data.add(new Tuple3<>(1,2,5));
data.add(new Tuple3<>(1,2,9));
data.add(new Tuple3<>(1,2,11));
data.add(new Tuple3<>(1,2,13));
DataStreamSource<Tuple3<Integer,Integer,Integer>> items = env.fromCollection(data);
```

#### 基于 Socket

通过监听 Socket 端口，我们可以在本地很方便地模拟一个实时计算环境。

StreamExecutionEnvironment 中提供了 socketTextStream 方法可以通过 host 和 port 从一个 Socket 中以文本的方式读取数据。

```
DataStream<String> text = env.socketTextStream("127.0.0.1", 9000, "\n");
```

#### 自定义 Source

我们可以通过实现 Flink 的SourceFunction 或者 ParallelSourceFunction 来实现单个或者多个并行度的 Source。

例如，我们在之前的课程中用到的：

```
public class MyStreamingSource implements SourceFunction<Item> {
    private boolean isRunning = true;
    /**
     * 重写run方法产生一个源源不断的数据发送源
     * @param ctx
     * @throws Exception
     */
    public void run(SourceContext<Item> ctx) throws Exception {
        while(isRunning){
            Item item = generateItem();
            ctx.collect(item);
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }
    @Override
    public void cancel() {
        isRunning = false;
    }
    //随机产生一条商品数据
    private Item generateItem(){
        int i = new Random().nextInt(100);
        ArrayList<String> list = new ArrayList();
        list.add("HAT");
        list.add("TIE");
        list.add("SHOE");
        Item item = new Item();
        item.setName(list.get(new Random().nextInt(3)));
        item.setId(i);
        return item;
    }
}

```

### 自带连接器
Flink 中支持了比较丰富的用来连接第三方的连接器，可以在官网中找到 Flink 支持的各种各样的连接器：

Apache Kafka (source/sink)

Apache Cassandra (sink)

Amazon Kinesis Streams (source/sink)

Elasticsearch (sink)

Hadoop FileSystem (sink)

RabbitMQ (source/sink)

Apache NiFi (source/sink)

Twitter Streaming API (source)

Google PubSub (source/sink)

需注意，我们在使用这些连接器时通常需要引用相对应的 Jar 包依赖。而且一定要注意，对于某些连接器比如 Kafka 是有版本要求的，一定要去官方网站找到对应的依赖版本。

### 基于 Apache Bahir 发布

Flink 还会基于 Apache Bahir 来发布一些 Connector，比如我们常用的 Redis 等。

Apache Bahir 的代码最初是从 Apache Spark 项目中提取的，后作为一个独立的项目提供。Apache Bahir 通过提供多样化的流连接器（Streaming Connectors）和 SQL 数据源扩展分析平台的覆盖面，最初只是为 Apache Spark 提供拓展。目前也为 Apache Flink 提供，后续还可能为 Apache Beam 和更多平台提供拓展服务。

我们可以在 Bahir 的首页中找到目前支持的 Flink 连接器：

Flink streaming connector for ActiveMQ

Flink streaming connector for Akka

Flink streaming connector for Flume

Flink streaming connector for InfluxDB

Flink streaming connector for Kudu

Flink streaming connector for Redis

Flink streaming connector for Netty

其中就有我们非常熟悉的 Redis，很多同学 Flink 项目中访问 Redis 的方法都是自己进行的实现，推荐使用 Bahir 连接器。

在本地单机情况下：

```
public static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>>{
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME");
    }
    @Override
    public String getKeyFromData(Tuple2<String, String> data) {
        return data.f0;
    }
    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        return data.f1;
    }
}
FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
DataStream<String> stream = ...;
stream.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper());

```

当然我们也可以使用在集群或者哨兵模式下使用 Redis 连接器。

集群模式：

```
FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
    .setNodes(new HashSet<InetSocketAddress>(Arrays.asList(new InetSocketAddress(5601)))).build();
DataStream<String> stream = ...;
stream.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper());
```

哨兵模式：



```
FlinkJedisSentinelConfig conf = new FlinkJedisSentinelConfig.Builder()
    .setMasterName("master").setSentinels(...).build();
DataStream<String> stream = ...;
stream.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper());
```



### 基于异步 I/O 和可查询状态
异步 I/O 和可查询状态都是 Flink 提供的非常底层的与外部系统交互的方式。

其中异步 I/O 是为了解决 Flink 在实时计算中访问外部存储产生的延迟问题，如果我们按照传统的方式使用 MapFunction，那么所有对外部系统的访问都是同步进行的。在很多情况下，计算性能受制于外部系统的响应速度，长时间进行等待，会导致整体吞吐低下。

我们可以通过继承 RichAsyncFunction 来使用异步 I/O：

```
/**
 * 实现 'AsyncFunction' 用于发送请求和设置回调
 */
class AsyncDatabaseRequest extends RichAsyncFunction<String, Tuple2<String, String>> {
    /** 能够利用回调函数并发发送请求的数据库客户端 */
    private transient DatabaseClient client;
    @Override
    public void open(Configuration parameters) throws Exception {
        client = new DatabaseClient(host, post, credentials);
    }
    @Override
    public void close() throws Exception {
        client.close();
    }
    @Override
    public void asyncInvoke(String key, final ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
        // 发送异步请求，接收 future 结果
        final Future<String> result = client.query(key);
        // 设置客户端完成请求后要执行的回调函数
        // 回调函数只是简单地把结果发给 future
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException e) {
                    // 显示地处理异常
                    return null;
                }
            }
        }).thenAccept( (String dbResult) -> {
            resultFuture.complete(Collections.singleton(new Tuple2<>(key, dbResult)));
        });
    }
}
// 创建初始 DataStream
DataStream<String> stream = ...;
// 应用异步 I/O 转换操作
DataStream<Tuple2<String, String>> resultStream =
    AsyncDataStream.unorderedWait(stream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100);
```

其中，ResultFuture 的 complete 方法是异步的，不需要等待返回。

我们在之前讲解 Flink State 时，提到过 Flink 提供了 StateDesciptor 方法专门用来访问不同的 state，StateDesciptor 同时还可以通过 setQueryable 使状态变成可以查询状态。可查询状态目前是一个 Beta 功能，暂时不推荐使用。

## 总结
这一课时讲解了 Flink 主要支持的 Source 和 Connector，这些是我们用 Flink 访问其他系统的桥梁。本节课也为我们寻找合适的连接器指明了方向。其中最重要的 Kafka 连接器我们将会在后面的实战课时中单独讲解。