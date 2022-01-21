## 第34讲：Flink 和 Redis 整合以及 Redis Sink 实现

[TOC]

上一课时我们使用了 3 种方法进行了 PV 和 UV 的计算，分别是全窗口内存统计、使用分组和过期数据剔除、使用 BitMap / 布隆过滤器。到此为止我们已经讲了从数据清洗到水印、窗口设计，PV 和 UV 的计算，接下来需要把结果写入不同的目标库供前端查询使用。

下面我们分别讲解 Flink 和 Redis/MySQL/HBase 是如何整合实现 Flink Sink 的。

### Flink Redis Sink
我们在第 27 课时，详细讲解过 Flink 使用 Redis 作为 Sink 的设计和实现，分别使用自定义 Redis Sink、开源的 Redis Connector 实现了写入 Redis。

在这里我们直接使用开源的 Redis 实现，首先新增 Maven 依赖如下：

```
<dependency> 
    <groupId>org.apache.flink</groupId> 
    <artifactId>flink-connector-redis_2.11</artifactId> 
    <version>1.1.5</version> 
</dependency> 
```

可以通过实现 RedisMapper 来自定义 Redis Sink，在这里我们使用 Redis 中的 HASH 作为存储结构，Redis 中的 HASH 相当于 Java 语言里面的 HashMap：

```
public class MyRedisSink implements RedisMapper<Tuple3<String,String, Integer>>{ 
    /** 
     * 设置redis数据类型 
     */ 
    @Override 
    public RedisCommandDescription getCommandDescription() { 
        return new RedisCommandDescription(RedisCommand.HSET,"flink_pv_uv"); 
    } 
    //指定key 
    @Override 
    public String getKeyFromData(Tuple3<String, String, Integer> data) { 
        return data.f1; 
    } 
    //指定value 
    @Override 
    public String getValueFromData(Tuple3<String, String, Integer> data) { 
        return data.f2.toString(); 
    } 
} 
```

上面实现了 RedisMapper 并覆写了其中的 getCommandDescription、getKeyFromData、getValueFromData 3 种方法，其中 getCommandDescription 定义了存储到 Redis 中的数据格式。这里我们定义的 RedisCommand 为 HSET，使用 Redis 中的 HASH 作为数据结构；getKeyFromData 定义了 HASH 的 Key；getValueFromData 定义了 HASH 的值。

然后我们直接调用 addSink 函数即可：

```
... 
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
            .process(new MyProcessWindowFunction()) 
            .addSink(new RedisSink<>(conf,new MyRedisSink())); 
... 
```

接下来讲解 Flink 和 MySQL 是如何整合实现 Flink Sink 的？

### Flink MySQL Sink
Flink 在最新版本 1.11 中支持了新的 JDBC Connector，我们可以直接在 Maven 中新增依赖：

```
<dependency> 
  <groupId>org.apache.flink</groupId> 
  <artifactId>flink-connector-jdbc_2.11</artifactId> 
  <version>1.11.0</version> 
</dependency> 
```

可以直接使用 JdbcSink 如下：

```
String driverClass = "com.mysql.jdbc.Driver"; 
String dbUrl = "jdbc:mysql://127.0.0.1:3306/test"; 
String userNmae = "root"; 
String passWord = "123456"; 

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
        .process(new MyProcessWindowFunction()) 
        .addSink( 
                JdbcSink.sink( 
                        "replace into pvuv_result (type,value) values (?,?)", 
                        (ps, value) -> { 
                            ps.setString(1, value.f1); 
                            ps.setInt(2,value.f2); 
                        }, 
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder() 
                                .withUrl(dbUrl) 
                                .withDriverName(driverClass) 
                                .withUsername(userNmae) 
                                .withPassword(passWord) 
                                .build()) 
                ); 
```


JDBC Sink 可以保证 "at-least-once" 语义保障，可通过实现“有则更新、无则写入”来实现写入 MySQL 的幂等性来实现 "exactly-once" 语义。

当然我们也可以自定义 MySQL Sink，直接继承 RichSinkFunction ：

```
public class MyMysqlSink extends RichSinkFunction<Person> { 
    private PreparedStatement ps = null; 
    private Connection connection = null; 
    String driver = "com.mysql.jdbc.Driver"; 
    String url = "jdbc:mysql://127.0.0.1:3306/test"; 
    String username = "root"; 
    String password = "123456"; 
    // 初始化方法 
    @Override 
    public void open(Configuration parameters) throws Exception { 
        super.open(parameters); 
        // 获取连接 
        connection = getConn(); 
        connection.setAutoCommit(false); 
    } 
    private Connection getConn() { 
        try { 
            Class.forName(driver); 
            connection = DriverManager.getConnection(url, username, password); 
        } catch (Exception e) { 
            e.printStackTrace(); 
        } 
        return connection; 
    } 
    //每一个元素的插入，都会调用一次 
    @Override 
    public void invoke(Tuple3<String,String,Integer> data, Context context) throws Exception { 
        ps.prepareStatement("replace into pvuv_result (type,value) values (?,?)") 
        ps.setString(1,data.f1); 
        ps.setInt(2,data.f2); 
        ps.execute(); 
        connection.commit(); 
    } 
    @Override 
    public void close() throws Exception { 
        super.close(); 
        if(connection != null){ 
            connection.close(); 
        } 
        if (ps != null){ 
            ps.close(); 
        } 
    } 
} 
```

我们通过重写 open、invoke、close 方法，数据写入 MySQL 时会首先调用 open 方法新建连接，然后调用 invoke 方法写入 MySQL，最后执行 close 方法关闭当前连接。

最后来讲讲 Flink 和 HBase 是如何整合实现 Flink Sink 的？

### Flink HBase Sink
HBase 也是我们经常使用的存储系统之一。

HBase 是一个分布式的、面向列的开源数据库，该技术来源于 Fay Chang 所撰写的 Google 论文“Bigtable：一个结构化数据的分布式存储系统”。就像 Bigtable 利用了 Google 文件系统（File System）所提供的分布式数据存储一样，HBase 在 Hadoop 之上提供了类似于 Bigtable 的能力。HBase 是 Apache 的 Hadoop 项目的子项目。HBase 不同于一般的关系数据库，它是一个适合于非结构化数据存储的数据库；另一个不同的是 HBase 基于列的而不是基于行的模式。

如果你对 HBase 不了解，可以参考官网给出的 快速入门。

Flink 没有提供直接连接 HBase 的连接器，我们通过继承 RichSinkFunction 来实现 HBase Sink。

首先，我们在 Maven 中新增以下依赖：

```
<dependency> 
    <groupId>org.apache.hbase</groupId> 
    <artifactId>hbase-client</artifactId> 
    <version>1.2.6.1</version> 
</dependency> 
<dependency> 
    <groupId>org.apache.hadoop</groupId> 
    <artifactId>hadoop-common</artifactId> 
    <version>2.7.5</version> 
</dependency> 
```

接下来通过继承 RichSinkFunction 覆写其中的 open、invoke、close 方法。代码如下：

```
public class MyHbaseSink extends RichSinkFunction<Tuple3<String, String, Integer>> { 
    private transient Connection connection; 
    @Override 
    public void open(Configuration parameters) throws Exception { 
        super.open(parameters); 
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create(); 
        conf.set(HConstants.ZOOKEEPER_QUORUM, "localhost:2181"); 
        connection = ConnectionFactory.createConnection(conf); 
    } 
    @Override 
    public void invoke(Tuple3<String, String, Integer> value, Context context) throws Exception { 
        String tableName = "database:pvuv_result"; 
        String family = "f"; 
        Table table = connection.getTable(TableName.valueOf(tableName)); 
        Put put = new Put(value.f0.getBytes()); 
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(value.f1),Bytes.toBytes(value.f2)); 
        table.put(put); 
        table.close(); 
    } 
    @Override 
    public void close() throws Exception { 
        super.close(); 
        connection.close(); 
    } 
} 
```

因为我们的程序是每 20 秒计算一次，并且输出，所以在写入 HBase 时没有使用批量方式。在实际的业务中，如果你的输出写入 HBase 频繁，那么推荐使用批量提交的方式。我们只需要稍微修改一下代码实现即可：

```
public class MyHbaseSink extends RichSinkFunction<Tuple3<String, String, Integer>> { 
    private transient Connection connection; 
    private transient List<Put> puts = new ArrayList<>(100); 
    @Override 
    public void open(Configuration parameters) throws Exception { 
        super.open(parameters); 
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create(); 
        conf.set(HConstants.ZOOKEEPER_QUORUM, "localhost:2181"); 
        connection = ConnectionFactory.createConnection(conf); 
    } 
    @Override 
    public void invoke(Tuple3<String, String, Integer> value, Context context) throws Exception { 
        String tableName = "database:pvuv_result"; 
        String family = "f"; 
        Table table = connection.getTable(TableName.valueOf(tableName)); 
        Put put = new Put(value.f0.getBytes()); 
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(value.f1),Bytes.toBytes(value.f2)); 
        puts.add(put); 
        if(puts.size() == 100){ 
            table.put(puts); 
            puts.clear(); 
        } 
        table.close(); 
    } 
    @Override 
    public void close() throws Exception { 
        super.close(); 
        connection.close(); 
    } 
} 
```

我们定义了一个容量为 100 的 List，每 100 条数据批量提交一次，可以大大提高写入效率。

## 总结
这节课我们学习了 Flink 计算 PV、UV后的结果分别写入 Redis、MySQL 和 HBase。我们在实际业务中可以选择使用不同的目标库，你可以在本文中找到对应的实现根据实际情况进行修改来使用。

