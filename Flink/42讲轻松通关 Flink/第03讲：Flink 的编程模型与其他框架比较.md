## 第03讲：Flink 的编程模型与其他框架比较

[TOC]

本课时我们主要介绍 Flink 的编程模型与其他框架比较。

本课时的内容主要介绍基于 Flink 的编程模型，包括 Flink 程序的基础处理语义和基本构成模块，并且和 Spark、Storm 进行比较，Flink 作为最新的分布式大数据处理引擎具有哪些独特的优势呢？

### Flink 的核心语义和架构模型

我们在讲解 Flink 程序的编程模型之前，先来了解一下 Flink 中的 Streams、State、Time 等核心概念和基础语义，以及 Flink 提供的不同层级的 API。

#### Flink 核心概念

- **Streams**（流），流分为有界流和无界流。有界流指的是有固定大小，不随时间增加而增长的数据，比如我们保存在 Hive 中的一个表；而无界流指的是数据随着时间增加而增长，计算状态持续进行，比如我们消费 Kafka 中的消息，消息持续不断，那么计算也会持续进行不会结束。
- **State**（状态），所谓的状态指的是在进行流式计算过程中的信息。一般用作容错恢复和持久化，流式计算在本质上是**增量计算**，也就是说需要不断地查询过去的状态。状态在 Flink 中有十分重要的作用，例如为了确保 Exactly-once 语义需要将数据写到状态中；此外，状态的持久化存储也是集群出现 Fail-over 的情况下自动重启的前提条件。
- **Time**（时间），Flink 支持了 Event time、Ingestion time、Processing time 等多种时间语义，时间是我们在进行 Flink 程序开发时判断业务状态是否滞后和延迟的重要依据。
- **API**：Flink 自身提供了不同级别的抽象来支持我们开发流式或者批量处理程序，由上而下可分为 SQL / Table API、DataStream API、ProcessFunction 三层，开发者可以根据需要选择不同层级的 API 进行开发。

#### Flink 编程模型和流式处理

我们在第 01 课中提到过，Flink 程序的基础构建模块是**流**（Streams）和**转换**（Transformations），每一个数据流起始于一个或多个 **Source**，并终止于一个或多个 **Sink**。数据流类似于**有向无环图**（DAG）。

![1.png](https://oss.ikeguang.com/image/202302081422360.png)

在分布式运行环境中，Flink 提出了**算子链**的概念，Flink 将多个算子放在一个任务中，由同一个线程执行，减少线程之间的切换、消息的序列化/反序列化、数据在缓冲区的交换，减少延迟的同时提高整体的吞吐量。

官网中给出的例子如下，在并行环境下，Flink 将多个 operator 的子任务链接在一起形成了一个task，每个 task 都有一个独立的线程执行。

![2.png](https://oss.ikeguang.com/image/202302081422597.png)

#### Flink 集群模型和角色

在实际生产中，Flink 都是以集群在运行，在运行的过程中包含了两类进程。

- **JobManager**：它扮演的是集群管理者的角色，负责调度任务、协调 checkpoints、协调故障恢复、收集 Job 的状态信息，并管理 Flink 集群中的从节点 TaskManager。
- **TaskManager**：实际负责执行计算的 Worker，在其上执行 Flink Job 的一组 Task；TaskManager 还是所在节点的管理员，它负责把该节点上的服务器信息比如内存、磁盘、任务运行情况等向 JobManager 汇报。
- **Client**：用户在提交编写好的 Flink 工程时，会先创建一个客户端再进行提交，这个客户端就是 Client，Client 会根据用户传入的参数选择使用 yarn per job 模式、stand-alone 模式还是 yarn-session 模式将 Flink 程序提交到集群。

![3.png](https://oss.ikeguang.com/image/202302081422652.png)

#### Flink 资源和资源组

在 Flink 集群中，一个 TaskManger 就是一个 JVM 进程，并且会用独立的线程来执行 task，为了控制一个 TaskManger 能接受多少个 task，Flink 提出了 Task Slot 的概念。

我们可以简单的把 Task Slot 理解为 TaskManager 的计算资源子集。假如一个 TaskManager 拥有 5 个 slot，那么该 TaskManager 的计算资源会被平均分为 5 份，不同的 task 在不同的 slot 中执行，避免资源竞争。但是需要注意的是，slot 仅仅用来做内存的隔离，对 CPU 不起作用。那么运行在同一个 JVM 的 task 可以共享 TCP 连接，减少网络传输，在一定程度上提高了程序的运行效率，降低了资源消耗。

![4.png](https://oss.ikeguang.com/image/202302081422783.png)

与此同时，Flink 还允许将不能形成算子链的两个操作，比如下图中的 flatmap 和 key&sink 放在一个 TaskSlot 里执行以达到资源共享的目的。

![5.png](https://oss.ikeguang.com/image/202302081422569.png)

### Flink 的优势及与其他框架的区别

Flink 在诞生之初，就以它独有的特点迅速风靡整个实时计算领域。在此之前，实时计算领域还有 Spark Streaming 和 Storm等框架，那么为什么 Flink 能够脱颖而出？我们将分别在架构、容错、语义处理等方面进行比较。

#### 架构

Stom 的架构是经典的主从模式，并且强依赖 ZooKeeper；Spark Streaming 的架构是基于 Spark 的，它的本质是微批处理，每个 batch 都依赖 Driver，我们可以把 Spark Streaming 理解为时间维度上的 Spark DAG。

Flink 也采用了经典的主从模式，DataFlow Graph 与 Storm 形成的拓扑 Topology 结构类似，Flink 程序启动后，会根据用户的代码处理成 Stream Graph，然后优化成为 JobGraph，JobManager 会根据 JobGraph 生成 ExecutionGraph。**ExecutionGraph 才是 Flink 真正能执行的数据结构**，当很多个 ExecutionGraph 分布在集群中，就会形成一张**网状的拓扑结**构。

#### 容错

Storm 在容错方面只支持了 Record 级别的 ACK-FAIL，发送出去的每一条消息，都可以确定是被成功处理或失败处理，因此 Storm 支持至少处理一次语义。

针对以前的 Spark Streaming 任务，我们可以配置对应的 checkpoint，也就是保存点。当任务出现 failover 的时候，会从 checkpoint 重新加载，使得数据不丢失。但是这个过程会导致原来的数据重复处理，不能做到“只处理一次”语义。

Flink 基于两阶段提交实现了精确的一次处理语义，我们将会在后面的课时中进行完整解析。

#### 反压（BackPressure）

反压是分布式处理系统中经常遇到的问题，当消费者速度低于生产者的速度时，则需要消费者将信息反馈给生产者使得生产者的速度能和消费者的速度进行匹配。

Stom 在处理背压问题上简单粗暴，当下游消费者速度跟不上生产者的速度时会直接通知生产者，生产者停止生产数据，这种方式的缺点是不能实现逐级反压，且调优困难。设置的消费速率过小会导致集群吞吐量低下，速率过大会导致消费者 OOM。

Spark Streaming 为了实现反压这个功能，在原来的架构基础上构造了一个“速率控制器”，这个“速率控制器”会根据几个属性，如任务的结束时间、处理时长、处理消息的条数等计算一个速率。在实现控制数据的接收速率中用到了一个经典的算法，即“PID 算法”。

Flink 没有使用任何复杂的机制来解决反压问题，Flink 在数据传输过程中使用了分布式阻塞队列。我们知道在一个阻塞队列中，当队列满了以后发送者会被天然阻塞住，这种阻塞功能相当于给这个阻塞队列提供了反压的能力。

### 总结

本课时主要介绍了 Flink 的核心语义和架构模型，并且从架构、容错、反压等多方位比较了 Flink 和其他框架的区别，为后面我们学习 Flink 的高级特性和实战打下了基础。

以上就是本课时的内容。在下一课时中，我将介绍“Flink 常用的 DataSet 和 DataStream API”，下一课时见。

[点击这里下载本课程源码](https://gitee.com/ddxygq/BigDataTechnical/tree/main/Flink)。

 

 