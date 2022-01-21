## 第38讲：Flink 调用 CEP 实现报警功能

[TOC]

上一课时中，我们详细讲解了 Flink CEP 中 Pattern 的分类，需要根据实际生产环境来选择单个模式、组合模式或者模式组。

在前面的课程中我们提到的三种典型场景下，分别根据业务需要实现了 Pattern 的定义，也可以根据自定义的 Pattern 检测到异常事件。那么接下来就需要根据检测到的异常事件发送告警，这一课将从这三种场景入手，来讲解完整的代码实现逻辑。

### 连续登录场景
在这个场景中，我们需要找出那些 5 秒钟内连续登录失败的账号，然后禁止用户，再次尝试登录需要等待 1 分钟。

我们定义的 Pattern 规则如下：

```
Pattern.<LogInEvent>begin("start").where(new IterativeCondition<LogInEvent>() {
    @Override
    public boolean filter(LogInEvent value, Context<LogInEvent> ctx) throws Exception {
        return value.getIsSuccess().equals("fail");
    }
}).next("next").where(new IterativeCondition<LogInEvent>() {
    @Override
    public boolean filter(LogInEvent value, Context<LogInEvent> ctx) throws Exception {
        return value.getIsSuccess().equals("fail");
    }
}).within(Time.seconds(5));
```

从登录消息 LogInEvent 可以得到用户登录是否成功，当检测到 5 秒钟内用户连续两次登录失败，则会发出告警消息，提示用户 1 分钟以后再试，或者这时候就需要前端输入验证码才能继续尝试。

首先我们模拟读取 Kafka 中的消息事件：

```
DataStream<LogInEvent> source = env.fromElements(
        new LogInEvent(1L, "fail", 1597905234000L),
        new LogInEvent(1L, "success", 1597905235000L),
        new LogInEvent(2L, "fail", 1597905236000L),
        new LogInEvent(2L, "fail", 1597905237000L),
        new LogInEvent(2L, "fail", 1597905238000L),
        new LogInEvent(3L, "fail", 1597905239000L),
        new LogInEvent(3L, "success", 1597905240000L)
).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator()).keyBy(new KeySelector<LogInEvent, Object>() {
    @Override
    public Object getKey(LogInEvent value) throws Exception {
        return value.getUserId();
    }
});
```



我们模拟了用户的登录信息，其中可以看到 ID 为 2 的用户连续三次登录失败。
时间戳和水印提取器的代码如下：

```
private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<LogInEvent>{
        private final long maxOutOfOrderness = 5000L;
        private long currentTimeStamp;
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimeStamp - maxOutOfOrderness);
        }
        @Override
        public long extractTimestamp(LogInEvent element, long previousElementTimestamp) {
            Long timeStamp = element.getTimeStamp();
            currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
            System.err.println(element.toString() + ",EventTime:" + timeStamp + ",watermark:" + (currentTimeStamp - maxOutOfOrderness));
            return timeStamp;
        }
    }
```

我们调用 Pattern.CEP 方法将 Pattern 和 Stream 结合在一起，在匹配到事件后先在控制台打印，并且向外发送。

```
PatternStream<LogInEvent> patternStream = CEP.pattern(source, pattern);
SingleOutputStreamOperator<AlertEvent> process = patternStream.process(new PatternProcessFunction<LogInEvent, AlertEvent>() {
    @Override
    public void processMatch(Map<String, List<LogInEvent>> match, Context ctx, Collector<AlertEvent> out) throws Exception {
        List<LogInEvent> start = match.get("start");
        List<LogInEvent> next = match.get("next");
        System.err.println("start:" + start + ",next:" + next);

        out.collect(new AlertEvent(String.valueOf(start.get(0).getUserId()), "出现连续登陆失败"));
    }
});
process.printToErr();
env.execute("execute cep");
```

我们右键运行，查看结果，如下图所示：

![Drawing 0.png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqc1F9DfmuARRpfAAMOQ71OEHc817.png)

可以看到报警事件的触发，其中 AlertEvent 是我们定义的报警事件。

```
public class AlertEvent {
    private String id;
    private String message;
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getMessage() {
        return message;
    }
    public void setMessage(String message) {
        this.message = message;
    }
    public AlertEvent(String id, String message) {
        this.id = id;
        this.message = message;
    }
    @Override
    public String toString() {
        return "AlertEvent{" +
                "id='" + id + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
```

对于 PatternProcessFunction 中的 processMatch 方法，第一个参数 Map<String, List> match，可以在源码中看到以下的注释：



match 这个 Map 的 Key 则是我们在 Pattern 中定义的 start 和 next。

### 交易活跃用户

在这个场景下，我们需要找出那些 24 小时内至少 5 次有效交易的账户。

在上一课时中我们的定义的规则如下：

```
Pattern.<TransactionEvent>begin("start").where(
        new SimpleCondition<TransactionEvent>() {
            @Override
            public boolean filter(TransactionEvent transactionEvent) {
                return transactionEvent.getAmount() > 0;
            }
        }
).timesOrMore(5)
 .within(Time.hours(24));
```

首先，我们模拟读取 Kafka 中的消息事件：

```
DataStream<TransactionEvent> source = env.fromElements(
        new TransactionEvent("100XX", 0.0D, 1597905234000L),
        new TransactionEvent("100XX", 100.0D, 1597905235000L),
        new TransactionEvent("100XX", 200.0D, 1597905236000L),
        new TransactionEvent("100XX", 300.0D, 1597905237000L),
        new TransactionEvent("100XX", 400.0D, 1597905238000L),
        new TransactionEvent("100XX", 500.0D, 1597905239000L),
        new TransactionEvent("101XX", 0.0D, 1597905240000L),
        new TransactionEvent("101XX", 100.0D, 1597905241000L)
).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator()).keyBy(new KeySelector<TransactionEvent, Object>() {
    @Override
    public Object getKey(TransactionEvent value) throws Exception {
        return value.getAccout();
    }
});
```

然后调用 Pattern.CEP 方法将 Pattern 和 Stream 结合在一起，我们在匹配到事件后先在控制台打印，并且向外发送。

```
PatternStream<TransactionEvent> patternStream = CEP.pattern(source, pattern);
    SingleOutputStreamOperator<AlertEvent> process = patternStream.process(new PatternProcessFunction<TransactionEvent, AlertEvent>() {
        @Override
        public void processMatch(Map<String, List<TransactionEvent>> match, Context ctx, Collector<AlertEvent> out) throws Exception {
            List<TransactionEvent> start = match.get("start");
            List<TransactionEvent> next = match.get("next");
            System.err.println("start:" + start + ",next:" + next);
            out.collect(new AlertEvent(start.get(0).getAccout(), "连续有效交易！"));
        }
    });
    process.printToErr();
    env.execute("execute cep");
}
```

最后右键运行查看结果，如下图所示：

![Drawing 2.png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgqCHl9DfoKAJoaeAAMWpTwyZAU138.png)

### 超时未支付
在这个场景中，我们需要找出那些下单后 10 分钟内没有支付的订单。

在上一课时中我们的定义的规则如下：

```
Pattern.<PayEvent>
        begin("begin")
        .where(new IterativeCondition<PayEvent>() {
            @Override
            public boolean filter(PayEvent payEvent, Context context) throws Exception {
                return payEvent.getAction().equals("create");
            }
        })
        .next("next")
        .where(new IterativeCondition<PayEvent>() {
            @Override
            public boolean filter(PayEvent payEvent, Context context) throws Exception {
                return payEvent.getAction().equals("pay");
            }
        })
        .within(Time.seconds(600));
OutputTag<PayEvent> orderTiemoutOutput = new OutputTag<PayEvent>("orderTimeout") {};
```

首先我们模拟读取 Kafka 中的消息事件：

```
DataStream<PayEvent> source = env.fromElements(
        new PayEvent(1L, "create", 1597905234000L),
        new PayEvent(1L, "pay", 1597905235000L),
        new PayEvent(2L, "create", 1597905236000L),
        new PayEvent(2L, "pay", 1597905237000L),
        new PayEvent(3L, "create", 1597905239000L)

).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator()).keyBy(new KeySelector<PayEvent, Object>() {
    @Override
    public Object getKey(PayEvent value) throws Exception {
        return value.getUserId();
    }
});
```



然后对匹配的结果进行分流，select 在这里有三个参数，第一个是超时消息的侧输出 Tag，第二个参数是超时消息的处理逻辑，第三个参数是正常的订单消息。

```
OutputTag<PayEvent> orderTimeoutOutput = new OutputTag<PayEvent>("orderTimeout") {};
Pattern<PayEvent, PayEvent> pattern = Pattern.<PayEvent>
        begin("begin")
        .where(new IterativeCondition<PayEvent>() {
            @Override
            public boolean filter(PayEvent payEvent, Context context) throws Exception {
                return payEvent.getAction().equals("create");
            }
        })
        .next("next")
        .where(new IterativeCondition<PayEvent>() {
            @Override
            public boolean filter(PayEvent payEvent, Context context) throws Exception {
                return payEvent.getAction().equals("pay");
            }
        })
        .within(Time.seconds(600));
PatternStream<PayEvent> patternStream = CEP.pattern(source, pattern);
SingleOutputStreamOperator<PayEvent> result = patternStream.select(orderTimeoutOutput, new PatternTimeoutFunction<PayEvent, PayEvent>() {
    @Override
    public PayEvent timeout(Map<String, List<PayEvent>> map, long l) throws Exception {
        return map.get("begin").get(0);
    }
}, new PatternSelectFunction<PayEvent, PayEvent>() {
    @Override
    public PayEvent select(Map<String, List<PayEvent>> map) throws Exception {
        return map.get("next").get(0);
    }
});
DataStream<PayEvent> sideOutput = result.getSideOutput(orderTimeoutOutput);
sideOutput.printToErr();
env.execute("execute cep");
```

最后右键运行查看结果，如下图所示：

到此为止，我们三种场景的完整代码实现就完成了。

## 总结
这一课时我们分别对连续登录、交易活跃用户、超时未支付三种业务场景进行了完整的代码实现，我们实际业务场景中可以根据本节课的内容灵活处理。

