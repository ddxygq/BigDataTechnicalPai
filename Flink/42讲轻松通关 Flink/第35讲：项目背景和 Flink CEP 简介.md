## 第35讲：项目背景和 Flink CEP 简介

从这一课时开始我们将进入“Flink CEP 实时预警系统”的学习，本课时先介绍项目的背景、架构设计。

### 背景
我们在第 11 课时“Flink CEP 复杂事件处理”已经介绍了 Flink CEP 的原理，它是 Flink 提供的复杂事件处理库，也是 Flink 提供的一个非常亮眼的功能，当然更是 Flink 中最难以理解的部分之一。

Complex Event Processing（CEP）允许我们在源源不断的数据中通过自定义的模式（Pattern）检测并且获取需要的数据，还可以对这些数据做对应的处理。Flink 提供了非常丰富的 API 来帮助我们实现非常复杂的模式进行数据匹配。

### Flink CEP 应用场景
CEP 在互联网各个行业都有应用，例如金融、物流、电商等行业，具体的作用如下。

实时监控：我们需要在大量的订单交易中发现那些虚假交易，在网站的访问日志中寻找那些使用脚本或者工具“爆破”登录的用户，或者在快递运输中发现那些滞留很久没有签收的包裹等。

风险控制：比如金融行业可以用来进行风险控制和欺诈识别，从交易信息中寻找那些可能存在危险交易和非法交易。

营销广告：跟踪用户的实时行为，指定对应的推广策略进行推送，提高广告的转化率。

当然了，还要很多其他的场景比如智能交通、物联网行业等，可以应用的场景不胜枚举。

### Flink CEP 的原理
如果你对 CEP 的理论基础非常感兴趣，推荐一篇论文“Efﬁcient Pattern Matching over Event Streams”。

Flink CEP 在运行时会将用户提交的代码转化成 NFA Graph，Graph 中包含状态（Flink 中 State 对象），以及连接状态的边（Flink 中 StateTransition 对象）。

Flink 中的每个模式都包含多个状态，我们进行模式匹配的过程就是进行状态转换的过程，在实际应用 Flink CEP 时，首先需要创建一系列的 Pattern，然后利用 NFACompiler 将 Pattern 进行拆分并且创建出 NFA，NFA 包含了 Pattern 中的各个状态和各个状态间转换的表达式。

我们用官网中的一个案例来讲解 Flink CEP 的应用：

```
DataStream<Event> input = ... 
Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where( 
        new SimpleCondition<Event>() { 
            @Override 
            public boolean filter(Event event) { 
                return event.getId() == 42; 
            } 
        } 
    ).next("middle").subtype(SubEvent.class).where( 
        new SimpleCondition<SubEvent>() { 
            @Override 
            public boolean filter(SubEvent subEvent) { 
                return subEvent.getVolume() >= 10.0; 
            } 
        } 
    ).followedBy("end").where( 
         new SimpleCondition<Event>() { 
            @Override 
            public boolean filter(Event event) { 
                return event.getName().equals("end"); 
            } 
         } 
    ); 
PatternStream<Event> patternStream = CEP.pattern(input, pattern); 
DataStream<Alert> result = patternStream.process( 
    new PatternProcessFunction<Event, Alert>() { 
        @Override 
        public void processMatch( 
                Map<String, List<Event>> pattern, 
                Context ctx, 
                Collector<Alert> out) throws Exception { 
            out.collect(createAlertFrom(pattern)); 
        } 
    }); 
```


在这个案例中可以看到程序结构如下：

第一步，定义一个模式 Pattern，在这里定义了一个这样的模式，即在所有接收到的事件中匹配那些以 ID 等于 42 的事件，然后匹配 volume 大于 10.0 的事件，继续匹配一个 name 等于 end 的事件；

第二步，匹配模式并且发出报警，根据定义的 pattern 在输入流上进行匹配，一旦命中我们的模式，就发出一个报警。

### 整体架构


我们在项目中定义特定事件附带各种上下文信息进入 Kafka，Flink 首先会消费这些信息过滤掉不需要的信息，然后会被我们定义好的模式进行处理，接着触发对应的规则；同时把触发规则的数据输出进行存储。

整个项目的设计可以分为下述几个部分：

- Flink CEP 源码解析和自定义消息事件

- 自定义 Pattern 和报警规则

- Flink 调用 CEP 实现报警功能

## 总结
本节课我们主要讲解了 Flink CEP 的应用场景和基本原理，在实际工作中，如果你的需求涉及从数据流中通过一定的规则识别部分数据，可以考虑使用 CEP。在接下来的课程中我们会分不同的课时来一一讲解这些知识点并进行实现

