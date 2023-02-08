####  Flink系列文章
1. [第01讲：Flink 的应用场景和架构模型](https://www.ikeguang.com/?p=1976)
2. [第02讲：Flink 入门程序 WordCount 和 SQL 实现](https://www.ikeguang.com/?p=1977)
3. [第03讲：Flink 的编程模型与其他框架比较](https://www.ikeguang.com/?p=1978)
4. [第04讲：Flink 常用的 DataSet 和 DataStream API](https://www.ikeguang.com/?p=1982)
5. [第05讲：Flink SQL & Table 编程和案例](https://www.ikeguang.com/?p=1983)
6. [第06讲：Flink 集群安装部署和 HA 配置](https://www.ikeguang.com/?p=1985)
7. [第07讲：Flink 常见核心概念分析](https://www.ikeguang.com/?p=1986)
8. [第08讲：Flink 窗口、时间和水印](https://www.ikeguang.com/?p=1987)
9. [第09讲：Flink 状态与容错](https://www.ikeguang.com/?p=1988)

我们在这一课时将讲解 Flink 常见的部署模式：本地模式、Standalone 模式和 Flink On Yarn 模式，然后分别讲解三种模式的使用场景和部署中常见的问题，最后将讲解在生产环境中 Flink 集群的高可用配置。

### Flink 常见的部署模式

#### 环境准备

在绝大多数情况下，我们的 Flink 都是运行在 Unix 环境中的，推荐在 Mac OS 或者 Linux 环境下运行 Flink。如果是集群模式，那么可以在自己电脑上安装虚拟机，保证有一个 master 节点和两个 slave 节点。

同时，要注意在所有的机器上都应该安装 JDK 和 SSH。JDK 是我们运行 JVM 语言程序必须的，而 SSH 是为了在服务器之间进行跳转和执行命令所必须的。关于服务器之间通过 SSH 配置公钥登录，你可以直接搜索安装和配置方法，我们不做过度展开。

Flink 的安装包可以在[这里](https://flink.apache.org/downloads.html)下载。需要注意的是，如果你要和 Hadoop 进行集成，那么我们需要使用到对应的 Hadoop 依赖，下面将会详细讲解。

#### Local 模式

Local 模式是 Flink 提供的最简单部署模式，一般用来本地测试和演示使用。

我们在[这里](https://flink.apache.org/downloads.html#apache-flink-1100)下载 [Apache Flink 1.10.0 for Scala 2.11](https://www.apache.org/dyn/closer.lua/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.11.tgz) 版本进行演示，该版本对应 Scala 2.11 版本。

将压缩包下载到本地，并且直接进行解压，使用 Flink 默认的端口配置，直接运行脚本启动：

```
➜  [SoftWare]# tar -zxvf flink-1.10.0-bin-scala_2.11.tgz
```

![image.png](https://oss.ikeguang.com/image/202302081427366.png)

上图则为解压完成后的目录情况。

然后，我们可以直接运行脚本启动 Flink ：

复制代码

```
➜  [flink-1.10.0]# ./bin/start-cluster.sh
```

![image (1).png](https://oss.ikeguang.com/image/202302081427974.png)

上图显示我们的 Flink 启动成功。

我们直接访问本地的 8081 端口，可以看到 Flink 的后台管理界面，验证 Flink 是否成功启动。

![image (2).png](https://oss.ikeguang.com/image/202302081427181.png)

可以看到 Flink 已经成功启动。当然，我们也可以查看运行日志来确认 Flink 是不是成功启动了，在 log 目录下有程序的启动日志：

![image (3).png](https://oss.ikeguang.com/image/202302081427750.png)

我们尝试提交一个测试任务：

复制代码

```
./bin/flink run examples/batch/WordCount.jar
```

![image (4).png](https://oss.ikeguang.com/image/202302081427266.png)

我们在控制台直接看到输出。同样，在 Flink 的后台管理界面 Completed Jobs 一栏可以看到刚才提交执行的程序：

![image (5).png](https://oss.ikeguang.com/image/202302081427384.png)

#### Standalone 模式

Standalone 模式是集群模式的一种，但是这种模式一般并不运行在生产环境中，原因和 on yarn 模式相比：

- Standalone 模式的部署相对简单，可以支持小规模，少量的任务运行；
- Stabdalone 模式缺少系统层面对集群中 Job 的管理，容易遭成资源分配不均匀；
- 资源隔离相对简单，任务之间资源竞争严重。

我们在 3 台虚拟机之间搭建 standalone 集群：

![图片1.png](https://oss.ikeguang.com/image/202302081428067.png)

在 master 节点，将 [Apache Flink 1.10.0 for Scala 2.11](https://www.apache.org/dyn/closer.lua/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.11.tgz) 包进行解压：

复制代码

```
[SoftWare]# tar -zxvf flink-1.10.0-bin-scala_2.11.tgz
```

**重点来啦**，我们需要修改 Flink 的配置文件，并且将修改好的解压目录完整的拷贝到两个从节点中去。在这里，我强烈建议主节点和从节点的目录要保持一致。

我们修改 conf 目录下的 flink-conf.yaml:

![image (6).png](https://oss.ikeguang.com/image/202302081428429.png)

flink-conf.yaml 文件中有大量的配置参数，我们挑选其中必填的最基本参数进行修改：

复制代码

```
jobmanager.rpc.address: master
jobmanager.heap.size: 1024m
jobmanager.rpc.port: 6123
taskmanager.memory.process.size: 1568m
taskmanager.numberOfTaskSlots: 1
parallelism.default: 1
jobmanager.execution.failover-strategy: region
io.tmp.dirs: /tmp
```

它们分别代表：

![图片2.png](https://oss.ikeguang.com/image/202302081428258.png)

如果你对其他的参数有兴趣的话，可以直接参考[官网](https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/ops/config.html)。接下来我们修改 conf 目录下的 master 和 slave 文件。vim master，将内容修改为：

```
master
```

vim slave，将内容修改为：

```
slave01
slave02
```

然后，将整个修改好的 Flink 解压目录使用 scp 远程拷贝命令发送到从节点：

```
scp -r /SoftWare/flink-1.10.0 slave01:/SoftWare/

scp -r /SoftWare/flink-1.10.0 slave02:/SoftWare/
```

在 master、slave01、slave02 上分别配置环境变量，vim /etc/profile，将内容修改为：

```
export FLINK_HOME=/SoftWare/flink-1.10.0
export PATH=$PATH:$FLINK_HOME/bin
```

到此为止，我们整个的基础配置已经完成，下面需要启动集群，登录 master 节点执行：

```
/SoftWare/flink-1.10.0/bin/start-cluster.sh
```

可以在浏览器访问：http://192.168.2.100:8081/ 检查集群是否启动成功。

集群搭建过程中，可能出现的问题：

- 端口被占用，我们需要手动杀掉占用端口的程序；
- 目录找不到或者文件找不到，我们在 flink-conf.yaml 中配置过 io.tmp.dirs ，这个目录需要手动创建。

#### On Yarn 模式和 HA 配置

![image (7).png](https://oss.ikeguang.com/image/202302081428782.png)

上图是 Flink on Yarn 模式下，Flink 和 Yarn 的交互流程。Yarn 是 Hadoop 三驾马车之一，主要用来做资源管理。我们在 Flink on Yarn 模式中也是借助 Yarn 的资源管理优势，需要在三个节点中配置 YARN_CONF_DIR、HADOOP_CONF_DIR、HADOOP_CONF_PATH 中的任意一个环境变量即可。

本课时中集群的高可用 HA 配置是基于独立的 ZooKeeper 集群。当然，Flink 本身提供了内置 ZooKeeper 插件，可以直接修改 conf/zoo.cfg，并且使用 /bin/start-zookeeper-quorum.sh 直接启动。

环境准备：

- ZooKeeper-3.x
- Flink-1.10.0
- Hadoop-2.6.5

我们使用 5 台虚拟机搭建 on yarn 的高可用集群：

![图片3.png](https://oss.ikeguang.com/image/202302081428373.png)

如果你在使用 Flink 的最新版本 1.10.0 时，那么需要在本地安装 Hadoop 环境并进行下面的操作。

首先，添加环境变量：

```
vi /etc/profile
# 添加环境变量
export HADOOP_CONF_DIR=/Software/hadoop-2.6.5/etc/hadoop
# 环境变量生效
source /etc/profile
```

其次，下载对应的的依赖包，并将对应的 Hadoop 依赖复制到 flink 的 lib 目录下，对应的 hadoop 依赖可以在[这里](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/)下载。

![image (8).png](https://oss.ikeguang.com/image/202302081428318.png)

与 standalone 集群不同的是，我们需要修改 flink-conf.yaml 文件中的一些配置：

```
high-availability: zookeeper
high-availability.storageDir: hdfs://cluster/flinkha/
high-availability.zookeeper.quorum: slave01:2181,slave02:2181,slave03:2181
```

它们分别代表：

![图片4.png](https://oss.ikeguang.com/image/202302081428860.png)

然后分别修改 master、slave、zoo.cfg 三个配置文件。
vim master，将内容修改为：

```
master01:8081
master02:8081
```

vim slave，将内容修改为：

```
slave01

slave02

slave03
```

vim zoo.cfg，将内容修改为：

```
server.1=slave01:2888:3888
server.2=slave02:2888:3888
server.3=slave03:2888:3888
```

然后，我们将整个修改好的 Flink 解压目录使用 scp 远程拷贝命令发送到从节点：

```
scp -r /SoftWare/flink-1.10.0 slave01:/SoftWare/
scp -r /SoftWare/flink-1.10.0 slave02:/SoftWare/
scp -r /SoftWare/flink-1.10.0 slave03:/SoftWare/
```

分别启动 Hadoop 和 ZooKeeper，然后在主节点，使用命令启动集群：

```
/SoftWare/flink-1.10.0/bin/start-cluster.sh
```

我们同样直接访问 http://192.168.2.100:8081/ 端口，可以看到 Flink 的后台管理界面，验证 Flink 是否成功启动。

在 Flink on yarn 模式下，启动集群的方式有两种：

- 直接在 yarn 上运行任务
- yarn session 模式

直接在 yarn 上运行任务相当于将 job 直接提交到 yarn 上，每个任务会根据用户的指定进行资源申请，任务之间互不影响。

```
./bin/flink run -yjm 1024m -ytm 4096m -ys 2  ./examples/batch/WordCount.jar
```

更多关于参数的含义，可以参考[官网](https://ci.apache.org/projects/flink/flink-docs-release-1.10/ops/cli.html)。使用 yarn session 模式，我们需要先启动一个 yarn-session 会话，相当于启动了一个 yarn 任务，这个任务所占用的资源不会变化，并且一直运行。我们在使用 flink run 向这个 session 任务提交作业时，如果 session 的资源不足，那么任务会等待，直到其他资源释放。当这个 yarn-session 被杀死时，所有任务都会停止。

例如我们启动一个 yarn session 任务，该任务拥有 8G 内存、32 个槽位。

```
./bin/yarn-session.sh -tm 8192 -s 32
```

我们在 yarn 的界面上可以看到这个任务的 ID，然后向这个 session ID 提交 Flink 任务：

```
./bin/flink run -m yarn-cluster -yid application_xxxx ./examples/batch/WordCount.jar
```

其中，application_xxxx 即为上述的 yarn session 任务 ID。

### 总结

本课时我们讲解了 Flink 的三种部署模式和高可用配置，并且对这三种部署模式的适用场景进行了讲解。在生产上，我们最常用的方式当然是 Flink on Yarn，借助 Yarn 在资源管理上的绝对优势，确保集群和任务的稳定。

>关注公众号：`大数据技术派`，回复`资料`，领取`1024G`资料。