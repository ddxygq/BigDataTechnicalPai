## 第27讲：Flink Redis Sink 实现

[TOC]

我们在第 12 课时“Flink 常用的 Source 和 Connector”中提过 Flink 提供了比较丰富的用来连接第三方的连接器，可以在官网中找到 Flink 支持的各种各样的连接器。

此外，Flink 还会基于 Apache Bahir 发布一些 Connector，其中就有我们非常熟悉的 Redis。很多人在 Flink 项目中访问 Redis 的方法都是自己进行实现的，我们也可以使用 Bahir 实现的 Redis 连接器。

事实上，使用 Redis Sink 常用的方法有很多，比如自定义 Sink 函数、依赖开源的 Redis Sink 实现等。

下面我们就分别介绍常用的 Redis Sink 实现。

### 自定义 Redis Sink

> REmote DIctionary Server（Redis）是一个由 Salvatore Sanfilippo 写的 key-value 存储系统。Redis 是一个开源的使用 ANSI C 语言编写、遵守 BSD 协议、支持网络、可基于内存亦可持久化的日志型、Key-Value 数据库，并提供多种语言的 API。
>
> 它通常被称为数据结构服务器，因为值（value）可以是字符串（String）、哈希（Hash）、列表（List）、集合（Sets）和有序集合（Sorted Sets）等类型。



如果你对 Redis 不熟悉，可以参考官网上的说明。 点击这里下载一个稳定版本的 Redis，我在本地安装的是 2.8.5 版本。使用下面命令进行安装：

```
wget http://download.redis.io/releases/redis-2.8.5.tar.gz
tar xzf redis-2.8.5.tar.gz
cd redis-2.8.5
make
```

然后进入 src 目录，就可以在本地启动一个 Redis 单机实例。

```
src/redis-server
```

![image (4).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqc1F8P95WAMtj9AAEeDYpGXyo212-20210223171957416.png)

我们可以使用 Redis 自带的交互命令来测试 Redis 实例，如下图所示：

![image (5).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgqCHl8P92iAflKfAABAnOh-e38133.png)

从上图可以看到，我们通过命令可以向 Redis 中设置数据。

然后在使用 Redis 时需要新增新的依赖，你可以点击这里根据 Redis 版本找到对应的 Redis 依赖：

```
<dependency>
   <groupId>redis.clients</groupId>
   <artifactId>jedis</artifactId>
   <version>2.8.2</version>
</dependency>
```

我们自定义一个 RedisSink 函数，继承 RichSinkFunction，重写其中的方法：

```
public class SelfRedisSink extends RichSinkFunction {
    private transient Jedis jedis;
    public void open(Configuration config) {
        jedis = new Jedis("localhost", 6379);
    }
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        if (!jedis.isConnected()) {
            jedis.connect();
        }
        jedis.set(value.f0, value.f1);
    }
    @Override
    public void close() throws Exception {
        jedis.close();
    }
}
```

上述代码，我们继承了 RichSinkFunction 并且实现了其中的 open、invoke 和 close 方法。其中 open 用于新建 Redis 客户端；invoke 函数将数据存储到 Redis 中，我们在这里将数据以字符串的形式存储到 Redis 中；close 函数用于使用完毕后关闭 Redis 客户端。

我们使用自定义 Redis Sink 的优点在于可以根据业务需求灵活处理，可以方便地使用 Jedis 提供的各种能力。

### 使用开源 Redis Connector
常用的开源 Connector 有两个，分别是 Flink 和 Bahir 提供的实现，其内部都是使用了 Java Redis 客户端 Jedis 实现了 Redis Sink。我们分别看一下它们的使用方法。

#### 第一种：Flink 提供的依赖包
新增以下依赖：

```
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-redis_2.11</artifactId>
    <version>1.1.5</version>
</dependency>
```

我们可以通过实现 RedisMapper 来自定义 Redis Sink：

```
public class RedisSink implements RedisMapper<Tuple2<String, String>>{
    /**
     * 设置 Redis 数据类型
     */
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);
    }
    /**
     * 设置Key
     */
    @Override
    public String getKeyFromData(Tuple2<String, String> data) {
        return data.f0;
    }
    /**
     * 设置value
     */
    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        return data.f1;
    }
}
```

我们自定义的 RedisSink 实现了 RedisMapper 并覆写了其中的 getCommandDescription、getKeyFromData、getValueFromData。其中 getCommandDescription 定义了存储到 Redis 中的数据格式，这里我们定义的 RedisCommand 为 SET，将数据以字符串的形式存储；getKeyFromData 定义了 SET 的 Key，getValueFromData 定义了 SET 的值。

完整的使用方式为：

```
public static void main(String[] args) throws Exception{
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Tuple2<String, String>> stream = env.fromElements("Flink","Spark","Storm").map(new MapFunction<String, Tuple2<String, String>>() {
        @Override
        public Tuple2<String, String> map(String s) throws Exception {
            return new Tuple2<>(s, s);
        }
    });
    FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
    stream.addSink(new RedisSink<>(conf, new RedisSink01()));
    env.execute("redis sink01");
}
```

我们可以在 Redis 的控制台中查询数据，如下图所示：

![image (10).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgqCHl8P94eAdc6BAABVXKIV0Sk143.png)

由图可知，我们设置了 Redis 中的数据。

#### 第二种：Bahir 提供的依赖包
新增以下依赖：

```
<dependency>
   <groupId>org.apache.bahir</groupId>
   <artifactId>flink-connector-redis_2.11</artifactId>
   <version>1.0</version>
</dependency>
```


目前该开源的 Bahir 实现最新版本是 1.1-snapshot，在这里使用的是 1.0 稳定版本。同样，我们也可以通过实现 RedisMapper 来自定义 Redis Sink：

```
public class RedisSink02 implements RedisMapper<Tuple2<String, String>> {
    /**
     * 设置 Redis 数据类型
     */
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);
    }
    /**
     * 设置 Key
     */
    @Override
    public String getKeyFromData(Tuple2<String, String> data) {
        return data.f0;
    }
    /**
     * 设置 value
     */
    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        return data.f1;
    }
}
```

和第一种方式一样，完整的使用方式如下代码所示：

```
public static void main(String[] args) throws Exception{
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Tuple2<String, String>> stream = env.fromElements("Flink","Spark","Storm").map(new MapFunction<String, Tuple2<String, String>>() {
        @Override
        public Tuple2<String, String> map(String s) throws Exception {
            return new Tuple2<>(s, s+"_sink2");
        }
    });
    FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
    stream.addSink(new RedisSink<>(conf, new RedisSink02()));
    env.execute("redis sink02");
}
```

我们可以在 Redis 的控制台中查询数据，如下图所示：

![image (11).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqc1F8P96WAPoW3AABHf0rerEo843.png)

开源实现的 Redis Connector 使用非常方便，但是有些功能缺失，例如，无法使用一些 Jedis 中的高级功能如设置过期时间等。

所以我们在实际生产中可以根据自己业务需要使用不同的实现。

## 总结
这一课时我们使用自定义 Redis Sink、开源的 Redis Connector 实现了写入 Redis，Redis Sink 的实现可以根据业务需要进行实现，Redis 以其极高的写入读取性能，是我们经常使用的 Sink 之一。学完本课时的内容后，你将掌握如何定义 Redis Sink 及其实现。

