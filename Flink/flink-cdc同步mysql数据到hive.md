## 什么是CDC？

CDC是（Change Data Capture 变更数据获取）的简称。核心思想是，监测并捕获数据库的变动（包括数据 或 数据表的插入INSERT、更新UPDATE、删除DELETE等），将这些变更按发生的顺序完整记录下来，写入到消息中间件中以供其他服务进行订阅及消费。

![Flink_CDC](https://ververica.github.io/flink-cdc-connectors/master/_images/flinkcdc.png)

## 1. 环境准备

- mysql

- Hive

- flink 1.13.5 on yarn

 说明：如果没有安装hadoop，那么可以不用yarn，直接用flink standalone环境吧。



## 2. 下载下列依赖包

下面两个地址下载flink的依赖包，放在lib目录下面。

1. [flink-sql-connector-hive-2.2.0_2.11-1.13.5.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.2.0_2.11/1.13.5/flink-sql-connector-hive-2.2.0_2.11-1.13.5.jar)

如果你的Flink是其它版本，可以来[这里](https://repo.maven.apache.org/maven2/org/apache/flink)下载。

说明：我hive版本是2.1.1，为啥这里我选择版本号是2.2.0呢，这是官方文档给出的版本对应关系：

| Metastore version | Maven dependency                 | SQL Client JAR                                               |
| :---------------- | :------------------------------- | :----------------------------------------------------------- |
| 1.0.0 - 1.2.2     | `flink-sql-connector-hive-1.2.2` | [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-1.2.2_2.11/1.13.6/flink-sql-connector-hive-1.2.2_2.11-1.13.6.jar) |
| 2.0.0 - 2.2.0     | `flink-sql-connector-hive-2.2.0` | [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.2.0_2.11/1.13.6/flink-sql-connector-hive-2.2.0_2.11-1.13.6.jar) |
| 2.3.0 - 2.3.6     | `flink-sql-connector-hive-2.3.6` | [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.6_2.11/1.13.6/flink-sql-connector-hive-2.3.6_2.11-1.13.6.jar) |
| 3.0.0 - 3.1.2     | `flink-sql-connector-hive-3.1.2` | [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2_2.11/1.13.6/flink-sql-connector-hive-3.1.2_2.11-1.13.6.jar) |

 官方文档地址在[这里](https://nightlies.apache.org/flink/flink-docs-release-1.13/zh/docs/connectors/table/hive/overview/)，可以自行查看。



## 3. 启动flink-sql client

1) 先在yarn上面启动一个application，进入flink13.5目录，执行：

```bash
bin/yarn-session.sh -d -s 2 -jm 1024 -tm 2048 -qu root.sparkstreaming -nm flink-cdc-hive
```



 2) 进入flink sql命令行

```bash
bin/sql-client.sh embedded -s flink-cdc-hive
```

![img](https://oss.ikeguang.com/image/202209131716843.png) 

 

## 4. 操作Hive

1） 首选创建一个catalog

```sql
CREATE CATALOG hive_catalog WITH (
    'type' = 'hive',
    'hive-conf-dir' = '/etc/hive/conf.cloudera.hive'
);
```

这里需要注意：hive-conf-dir是你的hive配置文件地址，里面需要有hive-site.xml这个主要的配置文件，你可以从hive节点复制那几个配置文件到本台机器上面。



2） 查询

此时我们应该做一些常规DDL操作，验证配置是否有问题：

```sql
use catalog hive_catalog;
show databases;
```

随便查询一张表

```sql
use test
show tables;
select * from people;
```

可能会报错：

![image-20220915183211513](https://oss.ikeguang.com/image/202209151832981.png)

把hadoop-mapreduce-client-core-3.0.0.jar放到flink的Lib目录下，这是我的，实际要根据你的hadoop版本对应选择。

注意：很关键，把这个jar包放到Lib下面后，需要重启application，然后重新用yarn-session启动一个application，因为我发现好像有缓存，把这个application kill 掉，重启才行：

![image-20220915183454691](https://oss.ikeguang.com/image/202209151834780.png)

然后，数据可以查询了，查询结果：

![image-20220915183102548](https://oss.ikeguang.com/image/202209151831033.png)



参考资料

https://nightlies.apache.org/flink/flink-docs-release-1.13/zh/docs/connectors/table/hive/hive_dialect/