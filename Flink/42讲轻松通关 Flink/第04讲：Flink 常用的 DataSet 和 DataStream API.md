##  第04讲：Flink 常用的 DataSet 和 DataStream API

[TOC]



本课时我们主要介绍 Flink 的 DataSet 和 DataStream 的 API，并模拟了实时计算的场景，详细讲解了 DataStream 常用的 API 的使用。

### 说好的流批一体呢

#### 现状

在前面的课程中，曾经提到过，Flink 很重要的一个特点是“流批一体”，然而事实上 Flink 并没有完全做到所谓的“流批一体”，即编写一套代码，可以同时支持流式计算场景和批量计算的场景。目前截止 1.10 版本依然采用了 DataSet 和 DataStream 两套 API 来适配不同的应用场景。

#### DateSet 和 DataStream 的区别和联系

在官网或者其他网站上，都可以找到目前 Flink 支持两套 API 和一些应用场景，但大都缺少了“为什么”这样的思考。

Apache Flink 在诞生之初的设计哲学是：**用同一个引擎支持多种形式的计算，包括批处理、流处理和机器学习等**。尤其是在流式计算方面，**Flink 实现了计算引擎级别的流批一体**。那么对于普通开发者而言，如果使用原生的 Flink ，直接的感受还是要编写两套代码。

整体架构如下图所示：

![image.png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqah16hTJCARnCYAALXFI10sJU200.png)
在 Flink 的源代码中，我们可以在 flink-java 这个模块中找到所有关于 DataSet 的核心类，DataStream 的核心实现类则在 flink-streaming-java 这个模块。

![image (1).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgoCgV6hTRuAdaYYAAfiA9_tU84430.png)

![image (2).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqah16hTSOAaofrAAd_Hyp6Zuw422.png)

在上述两张图中，我们分别打开 DataSet 和 DataStream 这两个类，可以发现，二者支持的 API 都非常丰富且十分类似，比如常用的 map、filter、join 等常见的 transformation 函数。

我们在前面的课时中讲过 Flink 的编程模型，对于 DataSet 而言，Source 部分来源于文件、表或者 Java 集合；而 DataStream 的 Source 部分则一般是消息中间件比如 Kafka 等。

由于 Flink DataSet 和 DataStream API 的高度相似，并且 Flink 在实时计算领域中应用的更为广泛。所以下面我们详细讲解 DataStream API 的使用。

### DataStream

我们先来回顾一下 Flink 的编程模型，在之前的课时中提到过，Flink 程序的基础构建模块是**流**（Streams）和**转换**（Transformations），每一个数据流起始于一个或多个 **Source**，并终止于一个或多个 **Sink**。数据流类似于**有向无环图**（DAG）。

![image (3).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqah16hTWOAYItJAADWU6-1xbw110.png)

在第 02 课时中模仿了一个流式计算环境，我们选择监听一个本地的 Socket 端口，并且使用 Flink 中的滚动窗口，每 5 秒打印一次计算结果。

#### 自定义实时数据源

在本课时中，我们利用 Flink 提供的自定义 Source 功能来实现一个自定义的实时数据源，具体实现如下：

```java
public class MyStreamingSource implements SourceFunction<MyStreamingSource.Item> {
    private boolean isRunning = true;
    /**
     * 重写run方法产生一个源源不断的数据发送源
     * @param ctx
     * @throws Exception
     */

    @Override
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
        Item item = new Item();
        item.setName("name" + i);
        item.setId(i);
        return item;
    }
    class Item{
        private String name;
        private Integer id;
        Item() {
        }

        public String getName() {

            return name;

        }



        void setName(String name) {

            this.name = name;

        }



        private Integer getId() {

            return id;

        }



        void setId(Integer id) {

            this.id = id;

        }



        @Override

        public String toString() {

            return "Item{" +

                    "name='" + name + '\'' +

                    ", id=" + id +

                    '}';

        }

    }

}





class StreamingDemo {

    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源

        DataStreamSource<MyStreamingSource.Item> text = 

        //注意：并行度设置为1,我们会在后面的课程中详细讲解并行度

        env.addSource(new MyStreamingSource()).setParallelism(1); 

        DataStream<MyStreamingSource.Item> item = text.map(

                (MapFunction<MyStreamingSource.Item, MyStreamingSource.Item>) value -> value);



        //打印结果

        item.print().setParallelism(1);

        String jobName = "user defined streaming source";

        env.execute(jobName);

    }



}
```

在自定义的数据源中，实现了 Flink 中的 SourceFunction 接口，同时实现了其中的 run 方法，在 run 方法中每隔一秒钟随机发送一个自定义的 Item。

可以直接运行 main 方法来进行测试：

![image (4).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqah16hTgOAZCaLAAcArI5AjtQ208.png)

可以在控制台中看到，已经有源源不断地数据开始输出。下面我们就用自定义的实时数据源来演示 DataStream API 的使用。

#### Map

Map 接受一个元素作为输入，并且根据开发者自定义的逻辑处理后输出。

![image (5).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqah16hThSAdYzhAADDHstaa9E625.png)

复制代码

```java
class StreamingDemo {

    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源

        DataStreamSource<MyStreamingSource.Item> items = env.addSource(new MyStreamingSource()).setParallelism(1); 

        //Map

        SingleOutputStreamOperator<Object> mapItems = items.map(new MapFunction<MyStreamingSource.Item, Object>() {

            @Override

            public Object map(MyStreamingSource.Item item) throws Exception {

                return item.getName();

            }

        });

        //打印结果

        mapItems.print().setParallelism(1);

        String jobName = "user defined streaming source";

        env.execute(jobName);

    }

}
```

我们只取出每个 Item 的 name 字段进行打印。

![image (6).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgoCgV6hTiuABREkAARA23HrkOQ888.png)

注意，**Map 算子是最常用的算子之一**，官网中的表述是对一个 DataStream 进行映射，每次进行转换都会调用 MapFunction 函数。从源 DataStream 到目标 DataStream 的转换过程中，返回的是 SingleOutputStreamOperator。当然了，我们也可以在重写的 map 函数中使用 lambda 表达式。

复制代码

```java
SingleOutputStreamOperator<Object> mapItems = items.map(

      item -> item.getName()

);
```

甚至，还可以自定义自己的 Map 函数。通过重写 MapFunction 或 RichMapFunction 来自定义自己的 map 函数。

复制代码

```java
class StreamingDemo {

    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源

        DataStreamSource<MyStreamingSource.Item> items = env.addSource(new MyStreamingSource()).setParallelism(1);

        SingleOutputStreamOperator<String> mapItems = items.map(new MyMapFunction());

        //打印结果

        mapItems.print().setParallelism(1);

        String jobName = "user defined streaming source";

        env.execute(jobName);

    }



    static class MyMapFunction extends RichMapFunction<MyStreamingSource.Item,String> {



        @Override

        public String map(MyStreamingSource.Item item) throws Exception {

            return item.getName();

        }

    }

}
```

此外，在 RichMapFunction 中还提供了 open、close 等函数方法，重写这些方法还能实现更为复杂的功能，比如获取累加器、计数器等。

#### FlatMap

FlatMap 接受一个元素，返回零到多个元素。FlatMap 和 Map 有些类似，但是当返回值是列表的时候，FlatMap 会将列表“平铺”，也就是以单个元素的形式进行输出。

复制代码

```java
SingleOutputStreamOperator<Object> flatMapItems = items.flatMap(new FlatMapFunction<MyStreamingSource.Item, Object>() {

    @Override

    public void flatMap(MyStreamingSource.Item item, Collector<Object> collector) throws Exception {

        String name = item.getName();

        collector.collect(name);

    }

});
```

上面的程序会把名字逐个输出。我们也可以在 FlatMap 中实现更为复杂的逻辑，比如过滤掉一些我们不需要的数据等。

#### Filter

顾名思义，Fliter 的意思就是过滤掉不需要的数据，每个元素都会被 filter 函数处理，如果 filter 函数返回 true 则保留，否则丢弃。

![image (7).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqah16hTtiAPWhJAADGVua1-cc867.png)

例如，我们只保留 id 为偶数的那些 item。

复制代码

```java
SingleOutputStreamOperator<MyStreamingSource.Item> filterItems = items.filter(new FilterFunction<MyStreamingSource.Item>() {

    @Override

    public boolean filter(MyStreamingSource.Item item) throws Exception {



        return item.getId() % 2 == 0;

    }

});
```

![image (8).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgoCgV6hTuWALqJwAATDpDg9dpY638.png)

当然，我们也可以在 filter 中使用 lambda 表达式：

复制代码

```java
SingleOutputStreamOperator<MyStreamingSource.Item> filterItems = items.filter( 

    item -> item.getId() % 2 == 0

);
```

#### KeyBy

在介绍 KeyBy 函数之前，需要你理解一个概念：**KeyedStream**。 在实际业务中，我们经常会需要根据数据的某种属性或者单纯某个字段进行分组，然后对不同的组进行不同的处理。举个例子，当我们需要描述一个用户画像时，则需要根据用户的不同行为事件进行加权；再比如，我们在监控双十一的交易大盘时，则需要按照商品的品类进行分组，分别计算销售额。

![image (9).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgoCgV6hTzyAUKHxAAF12IHd3bQ582.png)

我们在使用 KeyBy 函数时会把 DataStream 转换成为 KeyedStream，事实上 KeyedStream 继承了 DataStream，KeyedStream 中的元素会根据用户传入的参数进行分组。

我们在第 02 课时中讲解的 WordCount 程序，曾经使用过 KeyBy：

复制代码

```java
    // 将接收的数据进行拆分，分组，窗口计算并且进行聚合输出

        DataStream<WordWithCount> windowCounts = text

                .flatMap(new FlatMapFunction<String, WordWithCount>() {

                    @Override

                    public void flatMap(String value, Collector<WordWithCount> out) {

                        for (String word : value.split("\\s")) {

                            out.collect(new WordWithCount(word, 1L));

                        }

                    }

                })

                .keyBy("word")

                .timeWindow(Time.seconds(5), Time.seconds

                ....
```

在生产环境中使用 KeyBy 函数时要十分注意！该函数会把数据按照用户指定的 key 进行分组，那么相同分组的数据会被分发到一个 subtask 上进行处理，在大数据量和 key 分布不均匀的时非常容易出现数据倾斜和反压，导致任务失败。

![image (10).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqah16hT2CAUq4YAAIFumFqfTg398.png)

常见的解决方式是把所有**数据加上随机前后缀**，这些我们会在后面的课时中进行深入讲解。

#### Aggregations

Aggregations 为聚合函数的总称，常见的聚合函数包括但不限于 sum、max、min 等。Aggregations 也需要指定一个 key 进行聚合，官网给出了几个常见的例子：

复制代码

```java
keyedStream.sum(0);
keyedStream.sum("key");
keyedStream.min(0);
keyedStream.min("key");
keyedStream.max(0);
keyedStream.max("key");
keyedStream.minBy(0);
keyedStream.minBy("key");
keyedStream.maxBy(0);
keyedStream.maxBy("key");
```

在上面的这几个函数中，max、min、sum 会分别返回最大值、最小值和汇总值；而 minBy 和 maxBy 则会把最小或者最大的元素全部返回。我们拿 max 和 maxBy 举例说明：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//获取数据源

List data = new ArrayList<Tuple3<Integer,Integer,Integer>>();

data.add(new Tuple3<>(0,1,0));

data.add(new Tuple3<>(0,1,1));

data.add(new Tuple3<>(0,2,2));

data.add(new Tuple3<>(0,1,3));

data.add(new Tuple3<>(1,2,5));

data.add(new Tuple3<>(1,2,9));

data.add(new Tuple3<>(1,2,11));

data.add(new Tuple3<>(1,2,13));



DataStreamSource<MyStreamingSource.Item> items = env.fromCollection(data);

items.keyBy(0).max(2).printToErr();



//打印结果

String jobName = "user defined streaming source";

env.execute(jobName);
```

我们直接运行程序，会发现奇怪的一幕：

![image (11).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgoCgV6hT9SATGmCAATvGBf2FXg156.png)

从上图中可以看到，我们希望按照 Tuple3 的第一个元素进行聚合，并且按照第三个元素取最大值。结果如我们所料，的确是按照第三个元素大小依次进行的打印，但是结果却出现了一个这样的元素 (0,1,2)，这在我们的源数据中并不存在。

我们在 Flink 官网中的文档可以发现：

> The difference between min and minBy is that min returns the minimum value, whereas minBy returns the element that has the minimum value in this field (same for max and maxBy).

文档中说：**min 和 minBy 的区别在于，min 会返回我们制定字段的最大值，minBy 会返回对应的元素（max 和 maxBy 同理）**。

网上很多资料也这么写：min 和 minBy 的区别在于 min 返回最小的值，而 minBy 返回最小值的key，严格来说这是不正确的。

min 和 minBy 都会返回整个元素，只是 min 会根据用户指定的字段取最小值，并且把这个值保存在对应的位置，而对于其他的字段，并不能保证其数值正确。max 和 maxBy 同理。

事实上，对于 Aggregations 函数，Flink 帮助我们封装了状态数据，这些状态数据不会被清理，所以在实际生产环境中应该**尽量避免在一个无限流上使用 Aggregations**。而且，对于同一个 keyedStream ，只能调用一次 Aggregation 函数。

不建议的是那些状态无限增长的聚合，实际应用中一般会配合窗口使用。使得状态不会无限制扩张。

#### Reduce

Reduce 函数的原理是，会在每一个分组的 keyedStream 上生效，它会按照用户自定义的聚合逻辑进行分组聚合。
![image (12).png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgoCgV6hUAyAIaCwAAGkybBDznc114.png)

例如：

复制代码

```java
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

//items.keyBy(0).max(2).printToErr();



SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> reduce = items.keyBy(0).reduce(new ReduceFunction<Tuple3<Integer, Integer, Integer>>() {

    @Override

    public Tuple3<Integer,Integer,Integer> reduce(Tuple3<Integer, Integer, Integer> t1, Tuple3<Integer, Integer, Integer> t2) throws Exception {

        Tuple3<Integer,Integer,Integer> newTuple = new Tuple3<>();



        newTuple.setFields(0,0,(Integer)t1.getField(2) + (Integer) t2.getField(2));

        return newTuple;

    }

});



reduce.printToErr().setParallelism(1);
```

我们对下面的元素按照第一个元素进行分组，第三个元素分别求和，并且把第一个和第二个元素都置为 0：

复制代码

```java
data.add(new Tuple3<>(0,1,0));

data.add(new Tuple3<>(0,1,1));

data.add(new Tuple3<>(0,2,2));

data.add(new Tuple3<>(0,1,3));

data.add(new Tuple3<>(1,2,5));

data.add(new Tuple3<>(1,2,9));

data.add(new Tuple3<>(1,2,11));

data.add(new Tuple3<>(1,2,13));
```

那么最终会得到：(0,0,6) 和 (0,0,38)。

### 总结

这一课时介绍了常用的 API 操作，事实上 DataStream 的 API 远远不止这些，我们在看官方文档的时候要动手去操作验证一下，更为高级的 API 将会在实战课中用到的时候着重进行讲解。