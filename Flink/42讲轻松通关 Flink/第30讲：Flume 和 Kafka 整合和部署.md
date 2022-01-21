## 第30讲：Flume 和 Kafka 整合和部署

[TOC]

### Flume 概述
Flume 是 Hadoop 生态圈子中的一个重要组件，在上一课时中提过，它是一个分布式的、高可靠的、高可用的日志采集工具。

Flume 具有基于流式数据的简单灵活的架构，同时兼具高可靠性、高可用机制和故障转移机制。当我们使用 Flume 收集数据的速度超过下游的写入速度时，Flume 会自动做调整，使得数据的采集和推送能够平稳进行。

Flume 支持多路径采集、多管道数据接入和多管道数据输出。数据源可以是 HBase、HDFS 和文本文件，也可以是 Kafka 或者其他的 Flume 客户端。

#### Flume 的组件介绍
Flume 中有很多组件和概念，下面我把 Flume 中的核心组件一一进行介绍：



Client：客户端，用来运行 Flume Agent。

Event：Flume 中的数据单位，可以是一行日志、一条消息。

Agent：代表一个独立的 Flume 进程，包含三个组件：Source、Channel 和 Sink。

Source：数据的收集入口，用来获取 Event 并且传递给 Channel。

Channel：Event 的一个临时存储，是数据的临时通道，可以认为是一个队列。

Sink：从 Channel 中读取 Event，将 Event 中的数据传递给下游。

Flow：一个抽象概念，可以认为是一个从 Source 到达 Sink 的数据流向图。

#### Flume 本地环境搭建
我们在 Flume 的 官网下载安装包，在这里下载一个 1.8.0 的稳定版本，然后进行解压：
tar zxf apache-flume-1.8.0-bin.tar.gz 

![Drawing 1.png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqc1F8ejXGAQBp_AASfWl6sP68860.png)


可以看到有几个关键的目录，其中 conf/ 目录则是我们存放配置文件的目录。

#### Flume 测试
我们在下载 Flume 后，需要进行测试，可以通过监听本地的端口输入，并且在控制台进行打印。

首先，需要修改 conf/ 目录下的 flume-env.sh，在里面配置 JAVA_HOME 等配置：

```
cd /usr/local/apache-flume-1.8.0-bin/conf 
cp　flume-env.sh.template　flume-env.sh 
```

然后在 flume-env.sh 里面设置 JAVA_HOME 和 FLUME_CLASSPATH 变量：

```
export JAVA_HOME=/usr/local/java/jdk1.8 
FLUME_CLASSPATH="/usr/local/apache-flume-1.8.0-bin/" 
```

创建一个配置文件 nc_logger.conf :

```
# 定义这个 agent 中各组件的名字 
a1.sources = r1 
a1.sinks = k1 
a1.channels = c1 
# 描述和配置 source 组件：r1 
a1.sources.r1.type = netcat 
a1.sources.r1.bind = localhost 
a1.sources.r1.port = 9000 
# 描述和配置 sink 组件：k1 
a1.sinks.k1.type = logger 
# 描述和配置channel组件，此处使用是内存缓存的方式 
a1.channels.c1.type = memory 
a1.channels.c1.capacity = 1000 
a1.channels.c1.transactionCapacity = 100 
# 描述和配置 source channel sink 之间的连接关系 
a1.sources.r1.channels = c1 
a1.sinks.k1.channel = c1 
```

我们使用如下命令启动一个 Flume Agent:

```
bin/flume-ng agent  
-c conf  
-f conf/nc_logger.conf  
-n a1 -Dflume.root.logger=INFO,console 
```


其中有几个关键的命令：

–conf (-c) 用来指定配置文件夹路径；

–conf-file(-f) 用来指定采集方案文件；

–name(-n) 用来指定 agent 名字；

-Dflume.root.logger=INFO,console 开启 flume 日志输出到终端。

用 nc 命令打开本地 9000 端口：nc localhost 9000 


向端口输入几个单词，可以在另一端的控制台看到输出结果，如下图所示：

![Drawing 2.png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgqCHl8ejbqACoRbAAAux1bCItY816.png)

我们可以看到 9000 端口中输入的数据已经被打印出来了。



![Drawing 3.png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/Ciqc1F8ejdiAXvsmAAMGjSgfLtE499.png)

### Flume + Kafka 整合

![Drawing 4.png](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/CgqCHl8ejeCAA124AACyS8QBaJI555.png)


整体整合思路为，我们的两个 Flume Agent 分别部署在两台 Web 服务器上，用来采集两台服务器的业务日志，并且 Sink 到另一台 Flume Agent 上，然后将数据 Sink 到 Kafka 集群。在这里需要配置三个 Flume Agent。

首先在 Flume Agent 1 和 Flume Agent 2 上创建配置文件，具体如下。

修改 source、channel 和 sink 的配置，vim log_kafka.conf 代码如下：

```
# 定义这个 agent 中各组件的名字 
a1.sources = r1 
a1.sinks = k1 
a1.channels = c1 
# source的配置，监听日志文件中的新增数据 
a1.sources.r1.type = exec 
a1.sources.r1.command  = tail -F /home/logs/access.log 
#sink配置，使用avro日志做数据的消费 
a1.sinks.k1.type = avro 
a1.sinks.k1.hostname = flumeagent03 
a1.sinks.k1.port = 9000 
#channel配置，使用文件做数据的临时缓存 
a1.channels.c1.type = file 
a1.channels.c1.checkpointDir = /home/temp/flume/checkpoint 
a1.channels.c1.dataDirs = /home/temp/flume/data 
#描述和配置 source channel sink 之间的连接关系 
a1.sources.r1.channels = c1 
a1.sinks.k1.channel = c 
```

上述配置会监听 /home/logs/access.log 文件中的数据变化，并且将数据 Sink 到 flumeagent03 的 9000 端口。

然后我们分别启动 Flume Agent 1 和 Flume Agent 2，命令如下：

```
$ flume-ng agent  
-c conf  
-n a1  
-f conf/log_kafka.conf >/dev/null 2>&1 & 
```

第三个 Flume Agent 用来接收上述两个 Agent 的数据，并且发送到 Kafka。我们需要启动本地 Kafka，并且创建一个名为 log_kafka 的 Topic。

然后，我们创建 Flume 配置文件，具体如下。

修改 source、channel 和 sink 的配置，vim flume_kafka.conf 代码如下：

```
# 定义这个 agent 中各组件的名字 
a1.sources = r1 
a1.sinks = k1 
a1.channels = c1 
#source配置 
a1.sources.r1.type = avro 
a1.sources.r1.bind = 0.0.0.0 
a1.sources.r1.port = 9000 
#sink配置 
a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink 
a1.sinks.k1.topic = log_kafka 
a1.sinks.k1.brokerList = 127.0.0.1:9092 
a1.sinks.k1.requiredAcks = 1 
a1.sinks.k1.batchSize = 20 
#channel配置 
a1.channels.c1.type = memory 
a1.channels.c1.capacity = 1000 
a1.channels.c1.transactionCapacity = 100 
#描述和配置 source channel sink 之间的连接关系 
a1.sources.r1.channels = c1 
a1.sinks.k1.channel = c1     
```

配置完成后，我们启动该 Flume Agent：

```
$ flume-ng agent  
-c conf  
-n a1  
-f conf/flume_kafka.conf >/dev/null 2>&1 & 
```

我们在第 24 课时“Flink 消费 Kafka 数据开发” 中详细讲解了 Flink 消费 Kafka 数据的开发。当 Flume Agent 1 和 2 中监听到新的日志数据后，数据就会被 Sink 到 Kafka 指定的 Topic，我们就可以消费 Kafka 中的数据了。

### 总结
这一课时首先介绍了 Flume 和 Kafka 的整合和部署，然后详细介绍了 Flume 的组件并且搭建了 Flume 的本地环境进行测试，最后介绍了 Flume 和 Kafka 整合的配置文件编写。通过本课时的学习，相信你对 Flume 有了深入了解，在实际应用中可以正确配置 Flume Agent。