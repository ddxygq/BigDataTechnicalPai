## 第39讲：Flink 面试-基础篇

[TOc]

到目前为止，关于 Flink 的学习我们就告一段落了，接下来我们将进入最后一个面试模块的学习。在当前大背景下，面试这一关是求职者必须要面对的，也能从侧面考察对 Flink 的掌握情况，最后一模块将结合部分实际面试中出现的问题，进行详细分析，帮助大家顺利拿到 Offer。

最后一个面试模块分为了 4 个课时：

Flink 面试基础篇，包含了 Flink 的整体介绍、核心概念、算子等考察点；

Flink 面试进阶篇，包含了 Flink 中的数据传输、容错机制、序列化、数据热点、反压等实际生产环境中遇到的问题等考察点；

Flink 面试源码篇，包含了 Flink 的核心代码实现、Job 提交流程、数据交换、分布式快照机制、Flink SQL 的原理等考察点；

Flink 面试方案设计篇，主要是生产环境中常见业务场景下的解决方案设计。

本课时将分析 Flink 面试基础相关的面试题，总结出了经常面试的 12 道题。

### 面试题 1：请介绍一下 Flink。
这道题是一道很简单的入门题，考察我们队 Flink 整体的掌握情况，我们应该从以下几个基本的概念入手。

Flink 是大数据领域的分布式实时和离线计算引擎，其程序的基础构建模块是流（Streams）和转换（Transformations），每一个数据流起始于一个或多个 Source，并终止于一个或多个 Sink。数据流类似于有向无环图（DAG）。

Flink 提供了诸多高抽象层的 API 以便用户编写分布式任务：

DataSet API，对静态数据进行批处理操作，将静态数据抽象成分布式的数据集，用户可以方便地使用 Flink 提供的各种操作符对分布式数据集进行处理，支持 Java、Scala 和 Python；

DataStream API，对数据流进行流处理操作，将流式的数据抽象成分布式的数据流，用户可以方便地对分布式数据流进行各种操作，支持 Java 和 Scala；

Table API，对结构化数据进行查询操作，将结构化数据抽象成关系表，并通过类 SQL 的 DSL 对关系表进行各种查询操作，支持 Java 和 Scala。

此外，Flink 还针对特定的应用领域提供了领域库，例如，Flink ML，Flink 的机器学习库提供了机器学习 Pipelines API 并实现了多种机器学习算法；Gelly、Flink 的图计算库提供了图计算的相关 API 及多种图计算算法的实现。

### 面试题 2：Flink 的主要特性是什么？
这道题考察我们在使用 Flink 的过程中用到的哪些强大的特性。我们在开篇词和第 01 课时“Flink 的应用场景和架构模型”中提到过，Flink 的主要特性包括：批流一体、Exactly-Once、强大的状态管理等。

同时，Flink 还支持运行在包括 Yarn、Mesos、Kubernetes 在内的多种资源管理框架上。Flink 可以扩展到数千核心，其状态可以达到 TB 级别，且仍能保持高吞吐、低延迟的特性。

### 面试题 3：Flink 的适用场景有哪些？
这道题是一道很典型的考察我们掌握 Flink 的广度的问题，要求我们开发者不要局限在自己的一小块业务中，能主动地去学习其他人是如何玩转 Flink 的。我们可以从以下几个方面回答：

Flink 从诞生之初就因为其独特的流式计算特性迅速被各个公司采用到各行各业中，主要的应用场景包括：

实时数据计算

实时数据仓库和 ETL

事件驱动型场景，如告警、监控

此外，随着 Flink 对机器学习的支持越来越完善，还可以被用作机器学习和人工智能领域的引擎。

### 面试题 4：Flink 和 Spark Streaming 的异同点有哪些？

分析：这是一个非常宏观的面试题，因为两个框架的不同点非常之多。但是在面试时有非常重要的一点一定要回答出来：Flink 是标准的实时处理引擎，基于事件驱动；而 Spark Streaming 是微批（Micro-Batch）的模型。

在“03 | Flink 的编程模型与其他框架比较”课时中也讲过 Flink 的优势及与其他框架的区别。

#### 架构

Spark Streaming 的架构是基于 Spark 的，它的本质是微批处理，每个 batch 都依赖 Driver，我们可以把 Spark Streaming 理解为时间维度上的 Spark DAG。

Flink 也采用了经典的主从模式，DataFlow Graph 与 Storm 形成的拓扑 Topology 结构类似，Flink 程序启动后，会根据用户的代码处理成 Stream Graph，然后优化成为 JobGraph，JobManager 会根据 JobGraph 生成 ExecutionGraph。ExecutionGraph 才是 Flink 真正能执行的数据结构，当很多个 ExecutionGraph 分布在集群中，就会形成一张网状的拓扑结构。

#### 容错

针对 Spark Streaming 任务，我们可以配置对应的 Checkpoint，也就是保存点。当任务出现 Failover 的时候，会从 Checkpoint 重新加载，使得数据不丢失。但是这个过程会导致原来的数据重复处理，不能做到“只处理一次”语义。

Flink 基于两阶段提交实现了精确的一次处理语义，我们在第 14 课时 “Flink Exactly-once 实现原理解析” 中有过详细的讲解。

#### 反压（BackPressure）

反压是分布式处理系统中经常遇到的问题，当消费者速度低于生产者的速度时，则需要消费者将信息反馈给生产者使得生产者的速度能和消费者的速度进行匹配。

Spark Streaming 为了实现反压这个功能，在原来的架构基础上构造了一个“速率控制器”，“速率控制器”会根据几个属性，比如任务的结束时间、处理时长、处理消息的条数等计算一个速率。在实现控制数据的接收速率中用到了一个经典的算法，即“PID 算法”。

Flink 没有使用任何复杂的机制来解决反压问题，Flink 在数据传输过程中使用了分布式阻塞队列。我们知道在一个阻塞队列中，当队列满了以后发送者会被天然阻塞住，这种阻塞功能相当于给这个阻塞队列提供了反压的能力。

#### 时间机制

Spark Streaming 支持的时间机制有限，只支持处理时间。 Flink 支持了流处理程序在时间上的三个定义：处理时间、事件时间、注入时间；同时也支持 watermark 机制来处理滞后数据。

### 面试题 5：请谈谈 Flink 的组件栈和数据流模型。
Flink 的组件栈和数据流模型是我们使用 Flink 的基础，只有了解这些组件栈和数据模型才能入手进行 Flink 项目的开发。

![Drawing 0.png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgqCHl9IfgCATYDUAACIb65SOjo888.png)

Flink 自身提供了不同级别的抽象来支持我们开发流式或者批量处理程序，上图描述了 Flink 支持的 4 种不同级别的抽象。

对于我们开发者来说，大多数应用程序不需要上图中的最低级别的 Low-level 抽象，而是针对 Core API 编程，比如DataStream API（有界/无界流）和DataSet API（有界数据集）。这些流畅的 API 提供了用于数据处理的通用构建块，比如各种形式用户指定的转换、连接、聚合、窗口、状态等。

Table API 和 SQL是 Flink 提供的更为高级的 API 操作，Flink SQL 是 Flink 实时计算为简化计算模型，降低了用户使用实时计算门槛而设计的一套符合标准 SQL 语义的开发语言。

![Drawing 1.png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgqCHl9IfgeABLj2AACy7wUHmu4657.png)

Flink 程序的基本构建是数据输入来自一个 Source，Source 代表数据的输入端，经过 Transformation 进行转换，然后在一个或者多个 Sink 接收器中结束。数据流（Stream）就是一组永远不会停止的数据记录流，而转换（Transformation）是将一个或多个流作为输入，并生成一个或多个输出流的操作。在执行时，Flink 程序映射到 Streaming Dataflows，由流（Streams）和转换操作（Transformation Operators）组成。

### 面试题 6：请谈谈 Flink 中的角色和作用各是什么？
这道题考察我们生产环境中 Flink 的集群是如何工作的，开发者有没有主动去研究过生产环境 Flink 集群的部署过程，任务的启动流程等。

![Drawing 2.png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqc1F9Ifg-AXCKfAAP_OohSn6U306.png)

Flink 程序在运行时主要有 TaskManager、JobManager、Client 三种角色。其中 JobManager 扮演着集群中的管理者 Master 的角色，它是整个集群的协调者，负责接收 Flink Job、协调检查点、Failover 故障恢复等，同时管理 Flink 集群中从节点 TaskManager。

TaskManager 是实际负责执行计算的 Worker，在其上执行 Flink Job 的一组 Task，每个 TaskManager 负责管理其所在节点上的资源信息，比如内存、磁盘、网络，在启动的时候将资源的状态向 JobManager 汇报。

Client 是 Flink 程序提交的客户端，当用户提交一个 Flink 程序时，会先创建一个 Client，该 Client 首先会对用户提交的 Flink 程序进行预处理，然后提交到 Flink 集群中处理，所以 Client 需要从用户提交的 Flink 程序配置中获取 JobManager 的地址，并建立到 JobManager 的连接，将 Flink Job 提交给 JobManager。

### 面试题 7：请谈谈 Flink 中的计算资源 Task Slot。
Flink 中的计算资源是非常容易被忽略的一个知识点，它直接关系着我们线上 Flink 任务调优的原理和过程，变相的考察我们对于大数量下的 Flink 资源分配和调优是否熟悉。

在 Flink 集群中，一个 TaskManger 就是一个 JVM 进程，并且会用独立的线程来执行 Task，为了控制一个 TaskManger 能接受多少个 Task，Flink 提出了 Task Slot 的概念。

我们可以简单地把 Task Slot 理解为 TaskManager 的计算资源子集。假如一个 TaskManager 拥有 5 个 Slot，那么该 TaskManager 的计算资源会被平均分为 5 份，不同的 Task 在不同的 Slot 中执行，避免资源竞争。但需要注意的是，Slot 仅仅用来做内存的隔离，对 CPU 不起作用。那么运行在同一个 JVM 的 Task 可以共享 TCP 连接，以减少网络传输，在一定程度上提高了程序的运行效率，降低了资源消耗。

### 面试题 8：请谈谈 Flink 中的并行度设置
Flink 使用并行度来定义某一个算子被切分成多少个子任务。我们的 Flink 代码会被转换成逻辑视图，在实际运行时，根据用户的并行度配置会被转换成对应的子任务进行执行。

Flink 本身支持不同级别来设置我们任务并行度的方法，它们分别是：

算子级别

环境级别

客户端级别

集群配置级别

需要特别指出的是，设置并行度的优先级依次是：算子级别 > 环境级别 > 客户端级别 > 集群配置级别。

### 面试题 9：请谈谈 Flink 支持的集群规模情况。
这个面试题需要我们把生产环境中使用的集群规模、配置情况、Flink 版本等介绍清楚；同时说明部署模式（一般是 Flink on Yarn），除此之外，我们也可以同时在小集群（少于 5 个节点）和拥有 TB 级别状态的上千个节点上运行 Flink 任务。

### 面试题 10：请谈谈 Flink 中的时间分类。
这道题考察我们开发者对于 Flink 中的时间类型的掌握程度，Flink 中的时间是最基本的概念，我们任何一个实时计算任务都需要进行实践特性的选择。

我们在 “08 | Flink 窗口、时间和水印”课时中提到过 Flink 中的时间分为三种：

事件时间（Event Time），即事件实际发生的时间；

摄入时间（Ingestion Time），事件进入流处理框架的时间；

处理时间（Processing Time），事件被处理的时间。

### 面试题 11：请谈谈 Flink 中的水印。
Flink 中的水印是难度较高的一个知识点，这个问题考察开发者在实际生产中如何处理乱序问题的，是基于什么样的考虑选择不同的水印生成方式。

水印的出现是为了解决实时计算中的数据乱序问题，它的本质是 DataStream 中一个带有时间戳的元素。如果 Flink 系统中出现了一个 WaterMark T，那么就意味着 EventTime < T 的数据都已经到达，窗口的结束时间和 T 相同的那个窗口被触发进行计算了。

也就是说：水印是 Flink 判断迟到数据的标准，同时也是窗口触发的标记。

Flink 中的水印有两种：

- 周期生成水印 AssignerWithPeriodicWatermarks，周期默认的时间是 200ms；
- 按需要生成水印 PunctuatedWatermark，它适用于根据接收到的消息判断是否需要产生水印。

### 面试题 12：请谈谈 Flink 中的窗口。
这道题考察的是开发者在不同的业务场景中是如何选择时间窗口的，当然变相的就会和你的业务关联，为什么这样选择窗口？他有什么特点？

根据窗口数据划分的不同，目前 Flink 支持如下 3 种：

滚动窗口，窗口数据有固定的大小，窗口中的数据不会叠加；

滑动窗口，窗口数据有固定的大小，并且有生成间隔；

会话窗口，窗口数据没有固定的大小，根据用户传入的参数进行划分，窗口数据无叠加。

## 总结
本课时主要是 Flink 基础相关的面试题，对应前面的第 01 到 06 课时，你可以根据需要去对应的章节详细查看。另外，建议你要多多阅读对应知识点的源码，做到知其然且知其所以然。

