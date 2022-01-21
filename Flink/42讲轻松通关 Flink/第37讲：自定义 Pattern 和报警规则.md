## 第37讲：自定义 Pattern 和报警规则

[TOC]

在上一课时提过，PatternStream 是 Flink CEP 对模式匹配后流的抽象和定义，它把 DataStream 和 Pattern 组合到一起，并且基于 PatternStream 提供了一系列的方法，比如 select、process 等。

Flink CEP 的核心在于模式匹配，对于不同模式匹配特性的支持，往往决定相应的 CEP 框架是否能够得到广泛应用。那么 Flink CEP 对模式提供了哪些支持呢？

### Pattern 分类

Flink CEP 提供了 Pattern API 用于对输入流数据进行复杂事件规则的定义，用来提取符合规则的事件序列。

Flink 中的 Pattern 分为单个模式、组合模式、模式组 3 类。

#### 单个模式
复杂规则中的每一个单独的模式定义，就是个体模式。我们既可以定义一个给定事件出现的次数（量词），也可以定义一个条件来决定一个进来的事件是否被接受进入这个模式（条件）。

例如，我们对一个命名为 start 的模式，可以定义如下量词：

```
// 期望出现4次 
start.times(4); 
// 期望出现0或者4次 
start.times(4).optional(); 
// 期望出现2、3或者4次 
start.times(2, 4); 
// 期望出现2、3或者4次，并且尽可能地重复次数多 
start.times(2, 4).greedy(); 
// 期望出现0、2、3或者4次 
start.times(2, 4).optional(); 
// 期望出现0、2、3或者4次，并且尽可能地重复次数多 
start.times(2, 4).optional().greedy(); 
// 期望出现1到多次 
start.oneOrMore(); 
// 期望出现1到多次，并且尽可能地重复次数多 
start.oneOrMore().greedy(); 
// 期望出现0到多次 
start.oneOrMore().optional(); 
// 期望出现0到多次，并且尽可能地重复次数多 
start.oneOrMore().optional().greedy(); 
// 期望出现2到多次 
start.timesOrMore(2); 
// 期望出现2到多次，并且尽可能地重复次数多 
start.timesOrMore(2).greedy(); 
// 期望出现0、2或多次 
start.timesOrMore(2).optional(); 
// 期望出现0、2或多次，并且尽可能地重复次数多 
start.timesOrMore(2).optional().greedy();  

```

我们还可以定义需要的匹配条件：

```
start.where(new SimpleCondition<Event>() { 
    @Override 
    public boolean filter(Event value) { 
        return value.getName().startsWith("foo"); 
    } 
}); 
```

上面代码展示了定义一个以“foo”开头的事件。

当然我们还可以把条件组合到一起，例如，可以通过依次调用 where 来组合条件：

```
pattern.where(new SimpleCondition<Event>() { 
    @Override 
    public boolean filter(Event value) { 
        return ... // 一些判断条件 
    } 
}).or(new SimpleCondition<Event>() { 
    @Override 
    public boolean filter(Event value) { 
        return ... // 一些判断条件 
    } 
}); 
```

#### 组合模式
我们把很多单个模式组合起来，就形成了组合模式。Flink CEP 支持事件之间如下形式的连续策略：

- 严格连续，期望所有匹配的事件严格的一个接一个出现，中间没有任何不匹配的事件；
- 松散连续
- 不确定的松散连续

我们直接参考官网给出的案例：

```
// 严格连续 
Pattern<Event, ?> strict = start.next("middle").where(...); 
// 松散连续 
Pattern<Event, ?> relaxed = start.followedBy("middle").where(...); 
// 不确定的松散连续 
Pattern<Event, ?> nonDetermin = start.followedByAny("middle").where(...); 
// 严格连续的NOT模式 
Pattern<Event, ?> strictNot = start.notNext("not").where(...); 
// 松散连续的NOT模式 
Pattern<Event, ?> relaxedNot = start.notFollowedBy("not").where(...); 
```

对于上述的松散连续和不确定的松散连续这里举个例子来说明一下，例如，我们定义了模式“a b”，对于一个事件序列“a,c,b1,b2”，那么会产生下面的结果：

“a”和“b”之间严格连续则返回： {} （没有匹配）

“a”和“b”之间松散连续则返回： {a b1}

“a”和“b”之间不确定的松散连续则返回： {a b1}、{a b2}

#### 模式组
将一个模式作为条件嵌套在单个模式里，就是模式组。我们举例如下：

```
Pattern<Event, ?> start = Pattern.begin( 
    Pattern.<Event>begin("start").where(...).followedBy("start_middle").where(...) 
); 
// 严格连续 
Pattern<Event, ?> strict = start.next( 
    Pattern.<Event>begin("next_start").where(...).followedBy("next_middle").where(...) 
).times(3); 
// 松散连续 
Pattern<Event, ?> relaxed = start.followedBy( 
    Pattern.<Event>begin("followedby_start").where(...).followedBy("followedby_middle").where(...) 
).oneOrMore(); 
// 不确定松散连续 
Pattern<Event, ?> nonDetermin = start.followedByAny( 
    Pattern.<Event>begin("followedbyany_start").where(...).followedBy("followedbyany_middle").where(...) 
).optional(); 
```

### 自定义 Pattern
上一课时定义了几个不同的消息源，我们分别根据需求自定义匹配模式。

#### 第一个场景，连续登录场景

在这个场景中，我们需要找出那些 5 秒钟内连续登录失败的账号，然后禁止用户再次尝试登录需要等待 1 分钟。

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

### 第二个场景，超时未支付

在这个场景中，我们需要找出那些下单后 10 分钟内没有支付的订单。

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

在这里我们使用了侧输出流，并且将正常的订单流和超时未支付的超时流分开：

```
SingleOutputStreamOperator selectResult = patternStream.select(orderTiemoutOutput, 
        (PatternTimeoutFunction<PayEvent, ResultPayEvent>) (map, l) -> new ResultPayEvent(map.get("begin").get(0).getUserId(), "timeout"), 
        (PatternSelectFunction<PayEvent, ResultPayEvent>) map -> new ResultPayEvent(map.get("next").get(0).getUserId(), "success") 
); 
DataStream timeOutSideOutputStream = selectResult.getSideOutput(orderTiemoutOutput) 
```


第三个场景，找出交易活跃用户

在这个场景下，我们需要找出那些 24 小时内至少 5 次有效交易的账户。

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

## 总结
本一课时我们讲解了 Flink CEP 的模式匹配种类，并且基于上一课时的三个场景自定义了 Pattern，我们在实际生产中可以根据需求定义更为复杂的模式。