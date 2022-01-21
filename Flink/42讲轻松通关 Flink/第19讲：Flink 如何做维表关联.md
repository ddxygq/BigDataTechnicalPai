## 第19讲：Flink 如何做维表关联

[TOC]

在实际生产中，我们经常会有这样的需求，需要以原始数据流作为基础，然后关联大量的外部表来补充一些属性。例如，我们在订单数据中，希望能得到订单收货人所在省的名称，一般来说订单中会记录一个省的 ID，那么需要根据 ID 去查询外部的维度表补充省名称属性。

在 Flink 流式计算中，我们的一些维度属性一般存储在 MySQL/HBase/Redis 中，这些维表数据存在定时更新，需要我们根据业务进行关联。根据我们业务对维表数据关联的时效性要求，有以下几种解决方案：

- 实时查询维表
- 预加载全量数据
- LRU 缓存
- 分布式缓存
- 其他(广播)

上述几种关联外部维表的方式几乎涵盖了我们所有的业务场景，下面针对这几种关联维表的方式和特点一一讲解它们的实现方式和注意事项。

### 实时查询维表
实时查询维表是指用户在 Flink 算子中直接访问外部数据库，比如用 MySQL 来进行关联，这种方式是同步方式，数据保证是最新的。但是，当我们的流计算数据过大，会对外部系统带来巨大的访问压力，一旦出现比如连接失败、线程池满等情况，由于我们是同步调用，所以一般会导致线程阻塞、Task 等待数据返回，影响整体任务的吞吐量。而且这种方案对外部系统的 QPS 要求较高，在大数据实时计算场景下，QPS 远远高于普通的后台系统，峰值高达十万到几十万，整体作业瓶颈转移到外部系统。

这种方式的核心是，我们可以在 Flink 的 Map 算子中建立访问外部系统的连接。下面以订单数据为例，我们根据下单用户的城市 ID，去关联城市名称，核心代码实现如下：

```
public class Order {
    private Integer cityId;
    private String userName;
    private String items;
    public Integer getCityId() {
        return cityId;
    }
    public void setCityId(Integer cityId) {
        this.cityId = cityId;
    }
    public String getUserName() {
        return userName;
    }
    public void setUserName(String userName) {
        this.userName = userName;
    }
    public String getItems() {
        return items;
    }
    public void setItems(String items) {
        this.items = items;
    }
    @Override
    public String toString() {
        return "Order{" +
                "cityId=" + cityId +
                ", userName='" + userName + '\'' +
                ", items='" + items + '\'' +
                '}';
    }
}
public class DimSync extends RichMapFunction<String,Order> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DimSync.class);
    private Connection conn = null;
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/dim?characterEncoding=UTF-8", "admin", "admin");
    }
    public Order map(String in) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(in);
        Integer cityId = jsonObject.getInteger("city_id");
        String userName = jsonObject.getString("user_name");
        String items = jsonObject.getString("items");
        //根据city_id 查询 city_name
        PreparedStatement pst = conn.prepareStatement("select city_name from info where city_id = ?");
        pst.setInt(1,cityId);
        ResultSet resultSet = pst.executeQuery();
        String cityName = null;
        while (resultSet.next()){
            cityName = resultSet.getString(1);
        }
        pst.close();
        return new Order(cityId,userName,items,cityName);
    }
    public void close() throws Exception {
        super.close();
        conn.close();
    }
}
```

在上面这段代码中，RichMapFunction 中封装了整个查询维表，然后进行关联这个过程。需要注意的是，一般我们在查询小数据量的维表情况下才使用这种方式，并且要妥善处理连接外部系统的线程，一般还会用到线程池。最后，为了保证连接及时关闭和释放，一定要在最后的 close 方式释放连接，否则会将 MySQL 的连接数打满导致任务失败。

### 预加载全量数据
全量预加载数据是为了解决每条数据流经我们的数据系统都会对外部系统发起访问，以及对外部系统频繁访问而导致的连接和性能问题。这种思路是，每当我们的系统启动时，就将维度表数据全部加载到内存中，然后数据在内存中进行关联，不需要直接访问外部数据库。

这种方式的优势是我们只需要一次性地访问外部数据库，大大提高了效率。但问题在于，一旦我们的维表数据发生更新，那么 Flink 任务是无法感知的，可能会出现维表数据不一致，针对这种情况我们可以采取定时拉取维表数据。并且这种方式由于是将维表数据缓存在内存中，对计算节点的内存消耗很高，所以不能适用于数量很大的维度表。

我们还是用上面的场景，根据下单用户的城市 ID 去关联城市名称，核心代码实现如下：

```
public class WholeLoad extends RichMapFunction<String,Order> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WholeLoad.class);
    ScheduledExecutorService executor = null;
    private Map<String,String> cache;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    load();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        },5,5, TimeUnit.MINUTES);
    }
    @Override
    public Order map(String value) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        Integer cityId = jsonObject.getInteger("city_id");
        String userName = jsonObject.getString("user_name");
        String items = jsonObject.getString("items");
        String cityName = cache.get(cityId);
        return new Order(cityId,userName,items,cityName);
    }
    public void load() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/dim?characterEncoding=UTF-8", "admin", "admin");
        PreparedStatement statement = con.prepareStatement("select city_id,city_name from info");
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            String cityId = rs.getString("city_id");
            String cityName = rs.getString("city_name");
            cache.put(cityId, cityName);
        }
        con.close();
    }
}
```

在上面的例子中，我们使用 ScheduledExecutorService 每隔 5 分钟拉取一次维表数据。这种方式适用于那些实时场景不是很高，维表数据较小的场景。

### 异步IO(LRU 缓存)

LRU 是一种缓存算法，意思是最近最少使用的数据则被淘汰。在这种策略中，我们的维表数据天然的被分为冷数据和热数据。所谓冷数据指的是那些不经常使用的数据，热数据是那些查询频率高的数据。

对应到我们上面的场景中，根据城市 ID 关联城市的名称，北京、上海这些城市的订单远远高于偏远地区的一些城市，那么北京、上海就是热数据，偏远城市就是冷数据。这种方式存在一定的数据延迟，并且需要额外设置每条数据的失效时间。因为热点数据由于经常被使用，会常驻我们的缓存中，一旦维表发生变更是无法感知数据变化的。在这里使用 Guava 库提供的 CacheBuilder 来创建我们的缓存：

```
CacheBuilder.newBuilder()
        //最多存储10000条
        .maximumSize(10000)
        //过期时间为1分钟
        .expireAfterWrite(60, TimeUnit.SECONDS)
        .build();
```

整体的实现思路是：我们利用 Flink 的 RichAsyncFunction 读取 Hbase 的数据到缓存中，我们在关联维度表时先去查询缓存，如果缓存中不存在这条数据，就利用客户端去查询 Hbase，然后插入到缓存中。

首先我们需要一个 Hbase 的异步客户端：

```
<dependency>
    <groupId>org.hbase</groupId>
    <artifactId>asynchbase</artifactId>
    <version>1.8.2</version>
</dependency>
```

核心的代码实现如下：

```
public class LRU extends RichAsyncFunction<String,Order> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LRU.class);
    String table = "info";
    Cache<String, String> cache = null;
    private HBaseClient client = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //创建hbase客户端
        client = new HBaseClient("127.0.0.1","7071");
        cache = CacheBuilder.newBuilder()
                //最多存储10000条
                .maximumSize(10000)
                //过期时间为1分钟
                .expireAfterWrite(60, TimeUnit.SECONDS)
                .build();
    }
    @Override
    public void asyncInvoke(String input, ResultFuture<Order> resultFuture) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(input);
        Integer cityId = jsonObject.getInteger("city_id");
        String userName = jsonObject.getString("user_name");
        String items = jsonObject.getString("items");
        //读缓存
        String cacheCityName = cache.getIfPresent(cityId);
        //如果缓存获取失败再从hbase获取维度数据
        if(cacheCityName != null){
            Order order = new Order();
            order.setCityId(cityId);
            order.setItems(items);
            order.setUserName(userName);
            order.setCityName(cacheCityName);
            resultFuture.complete(Collections.singleton(order));
        }else {
            client.get(new GetRequest(table,String.valueOf(cityId))).addCallback((Callback<String, ArrayList<KeyValue>>) arg -> {
                for (KeyValue kv : arg) {
                    String value = new String(kv.value());
                    Order order = new Order();
                    order.setCityId(cityId);
                    order.setItems(items);
                    order.setUserName(userName);
                    order.setCityName(value);
                    resultFuture.complete(Collections.singleton(order));
                    cache.put(String.valueOf(cityId), value);
                }
                return null;
            });
        }
    }
}
```

这里需要特别注意的是，我们用到了异步 IO (RichAsyncFunction)，这个功能的出现就是为了解决与外部系统交互时网络延迟成为系统瓶颈的问题。

我们在流计算环境中，在查询外部维表时，假如访问是同步进行的，那么整体能力势必受限于外部系统。正是因为异步 IO 的出现使得访问外部系统可以并发的进行，并且不需要同步等待返回，大大减轻了因为网络等待时间等引起的系统吞吐和延迟问题。

我们在使用异步 IO 时，一定要使用异步客户端，如果没有异步客户端我们可以自己创建线程池模拟异步请求。

### 分布式缓存

### 其他(广播)

除了上述常见的处理方式，我们还可以通过将维表消息广播出去，或者自定义异步线程池访问维表，甚至还可以自己扩展 Flink SQL 中关联维表的方式直接使用 SQL Join 方法关联查询结果。

总体来讲，关联维表的方式就以上几种方式，并且基于这几种方式还会衍生出各种各样的解决方案。我们在评价一个方案的优劣时，应该从业务本身出发，不同的业务场景下使用不同的方式。

## 总结

这一课时我们讲解了 Flink 关联维度表的几种常见方式，分别介绍了它们的优劣和适用场景，并进行了代码实现。我们在实际生产中应该从业务本身出发来评估每种方案的优劣，从而达到维表关联在时效性和性能上达到最优。

