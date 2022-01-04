>关注公众号：`大数据技术派`，回复`资料`，领取`1024G`资料。
# 数据迁移使用场景
- 冷热集群数据分类存储,详见上述描述.
- 集群数据整体搬迁.当公司的业务迅速的发展,导致当前的服务器数量资源出现临时紧张的时候,为了更高效的利用资源,会将原A机房数据整体迁移到B机房的,原因可能是B机房机器多,而且B机房本身开销较A机房成本低些等.
- 数据的准实时同步.数据的准实时同步与上一点的不同在于第二点可以一次性操作解决,而准实时同步需要定期同步,而且要做到周期内数据基本完全一致.数据准实时同步的目的在于数据的双备份可用,比如某天A集群突然宣告不允许再使用了,此时可以将线上使用集群直接切向B的同步集群,因为B集群实时同步A集群数据,拥有完全一致的真实数据和元数据信息,所以对于业务方使用而言是不会受到任何影响的.


# hadoop 集群间拷贝数据：
需要将数据源集群的/etc/hosts中的hadoop节点拷贝到目标集群所有节点的/etc/hosts中，保证新集群所有节点可以ping同老集群所有节点；

```
hadoop distcp hdfs://qcloud-hadoop02:9000/hive/warehouse/hm2.db/helper/dt=2018-10-17 /data
```
**说明**：我们这里是apache hadoop 到cdh数据迁移，这个命令仍然是可以用的。


## 一般用法
1、迁移之前需要把两个集群的所有节点都互通/etc/hosts文件（重要，包括各个数据节点）

2、配置当前集群主节点到老集群各个节点的ssh免密登陆(可选)

3、由于老集群是HDP2.7.1，新集群是cdh5.8.5,版本不同，不能用hdfs协议直接拷贝，需要用http协议
即不能用：```distcp hdfs://src:50070/foo /user```
而要用：```distcp hftp://src:50070/foo /user```
最终的命令为：
```
hadoop distcp hftp://192.168.57.73:50070/hive3/20171008 /hive3/
```
4、如果两个集群的版本相同，则可以使用hdfs协议，命令如下：
```
hadoop distcp hdfs://namenodeip:9000/foo hdfs://namenodeip:9000/foo
```
5、由于迁移数据运行了mr任务，对集群资源有一定的消耗


# DistCp优势特性
- 1 带宽限流

DistCp是支持带宽限流的,使用者可以通过命令参数bandwidth来为程序进行限流,原理类似于HDFS中数据Balance程序的限流.

---
- 2 增量数据同步

对于增量数据同步的需求,在DistCp中也得到了很好的实现.通过update,append和*diff*2个参数能很好的解决.官方的参数使用说明:

**Update**: Update target, copying only missing files or directories <br>
**Append**: Reuse existing data in target files and append new data to them if possible.<br>
**Diff**: Use snapshot diff report to identify the difference between source and target.<br>

**第一个参数,解决了新增文件目录的同步;第二参数,解决已存在文件的增量更新同步;第三个参数解决删除或重命名文件的同步**.<br>
这里需要额外解释一下diff的使用需要设置2个不同时间的snapshot进行对比,产生相应的DiffInfo.在获取快照文件的变化时,只会选择出DELETE和RENAME这2种类型的变化信息.

**相同hadoop版本同步数据**
```
hadoop distcp -skipcrccheck -update -m 20 hdfs://dchadoop002.dx:8020/user/dc/warehouse/test /user/dc/warehouse/test
```
**不同hadoop版本同步数据**
```
hadoop distcp -skipcrccheck -update -m 20 hftp://ns1/user/test /user/dc/test
```
参数:
```
-m 表示并发数
-skipcrccheck 跳过hdfs校验
-update 更新文件
```

**理源路径的方式与默认值不同，有些细节需要注意。**
这里给出一些 -update和 -overwrite的例子。考虑从/source/first/ 和 /source/second/ 到 /target/的拷贝，源路径包括：
```
hdfs://nn1:8020/source/first/1
hdfs://nn1:8020/source/first/2
hdfs://nn1:8020/source/second/10
hdfs://nn1:8020/source/second/20
```
当不使用-update或-overwrite选项时，DistCp默认会在/target下创建/first和/second目录。因此将在/target之前先创建目录。

从而：
```
hadoop distcp hdfs://nn1:8020/source/first hdfs://nn1:8020/source/second hdfs://nn2:8020/target
```
上述命令将在/target中生成以下内容：
```
hdfs://nn2:8020/target/first/1
hdfs://nn2:8020/target/first/2
hdfs://nn2:8020/target/second/10
hdfs://nn2:8020/target/second/20
```
**当指定-update或-overwrite时，源目录的内容将复制到目标，而不是源目录本身**。 

从而：
```
distcp -update hdfs://nn1:8020/source/first hdfs://nn1:8020/source/second hdfs://nn2:8020/target
```
上述命令将在/ target中生成以下内容：
```
hdfs://nn2:8020/target/1
hdfs://nn2:8020/target/2
hdfs://nn2:8020/target/10
hdfs://nn2:8020/target/20
```
如果设置了这两个选项，每个源目录的内容都会和目标目录的内容做比较。如果两个源文件夹都包含一个具有相同名称的文件（例如“0”），那么这两个源文件将在目的地映射到同一个目录：/target/0。DistCp碰到这类冲突的情况会终止操作并退出。
现在，请考虑以下复制操作：
```
distcp hdfs://nn1:8020/source/first hdfs://nn1:8020/source/second hdfs://nn2:8020/target
```
其中源路径/大小:
```
hdfs://nn1:8020/source/first/1 32
hdfs://nn1:8020/source/first/2 32
hdfs://nn1:8020/source/second/10 64
hdfs://nn1:8020/source/second/20 32
```
和目的路径/大小:
```
hdfs://nn2:8020/target/1 32
hdfs://nn2:8020/target/10 32
hdfs://nn2:8020/target/20 64
```
会产生:
```
hdfs://nn2:8020/target/1 32
hdfs://nn2:8020/target/2 32
hdfs://nn2:8020/target/10 64
hdfs://nn2:8020/target/20 32
```
文件“1”因为文件长度和内容匹配而被跳过。
文件“2”被复制，因为它不存在/target中。因为目标文件内容与源文件内容不匹配，文件“10”和文件“20”被覆盖。如果使用-update
选项，文件“1”也被覆盖。

```
```


- 3 高效的性能

执行的分布式特性<br>
高效的MR组件


## hive数据迁移
**1.hive数据export到hdfs**
```
export table hm2.helper to '/tmp/export/hm2/helper';
```
如下：
```
hive> export table hm2.helper to '/tmp/export/hm2/helper';
Copying data from file:/app/data/hive/tmp/scratchdir/ce4c15d9-6875-40ed-add4-deedd75a4a92/hive_2018-10-26_10-58-21_552_8465737459112285307-1/-local-10000/_metadata
Copying file: file:/app/data/hive/tmp/scratchdir/ce4c15d9-6875-40ed-add4-deedd75a4a92/hive_2018-10-26_10-58-21_552_8465737459112285307-1/-local-10000/_metadata
Copying data from hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=13/msgtype=helper
Copying data from hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00001
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00003
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00004
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00005
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00006
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00007
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00008
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00009
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00010
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00011
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00012
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00013
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00014
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00015
Copying data from hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-13/hour=13/msgtype=helper
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-13/hour=13/msgtype=helper/part-m-00002
Copying data from hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-13/hour=14/msgtype=helper
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-13/hour=14/msgtype=helper/part-m-00000
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-13/hour=14/msgtype=helper/part-m-00002
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-13/hour=14/msgtype=helper/part-m-00006
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-13/hour=14/msgtype=helper/part-m-00016
Copying data from hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-22/hour=08/msgtype=helper
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-22/hour=08/msgtype=helper/part-m-00006
Copying data from hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-22/hour=09/msgtype=helper
Copying file: hdfs://nameser/hive/warehouse/hm2.db/helper/dt=2018-06-22/hour=09/msgtype=helper/part-m-00000
OK
Time taken: 1.52 seconds
```
**2.集群间数据复制**

需要保证原始集群目录有读权限，新的集群复制保存目录有写权限:
```
两个集群都要赋权
hdfs dfs -chmod -R 777 /tmp/export/*
hdfs dfs -chmod -R 777 /tmp/export/*
```
数据复制
```
hadoop distcp hdfs://qcloud-test-hadoop01:9000/tmp/export/hm2 /tmp/export
```
**3.数据导入hive**

在源hive ```show create table tbName```显示建表语句，用语句在目标hive建表，然后倒入数据：
```
import table hm2.helper from '/tmp/export/hm2/helper';
```
成功：
```
hive> import table hm2.helper from '/tmp/export/hm2/helper';
Copying data from hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=13/msgtype=helper
Copying data from hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00001
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00003
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00004
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00005
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00006
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00007
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00008
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00009
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00010
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00011
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00012
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00013
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00014
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-12/hour=14/msgtype=helper/part-m-00015
Copying data from hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-13/hour=13/msgtype=helper
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-13/hour=13/msgtype=helper/part-m-00002
Copying data from hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-13/hour=14/msgtype=helper
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-13/hour=14/msgtype=helper/part-m-00000
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-13/hour=14/msgtype=helper/part-m-00002
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-13/hour=14/msgtype=helper/part-m-00006
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-13/hour=14/msgtype=helper/part-m-00016
Copying data from hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-22/hour=08/msgtype=helper
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-22/hour=08/msgtype=helper/part-m-00006
Copying data from hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-22/hour=09/msgtype=helper
Copying file: hdfs://qcloud-cdh01.2144.com:8020/tmp/export/hm2/helper/dt=2018-06-22/hour=09/msgtype=helper/part-m-00000
Loading data to table hm2.helper partition (dt=2018-06-12, hour=13, msgtype=helper)
Loading data to table hm2.helper partition (dt=2018-06-12, hour=14, msgtype=helper)
Loading data to table hm2.helper partition (dt=2018-06-13, hour=13, msgtype=helper)
Loading data to table hm2.helper partition (dt=2018-06-13, hour=14, msgtype=helper)
Loading data to table hm2.helper partition (dt=2018-06-22, hour=08, msgtype=helper)
Loading data to table hm2.helper partition (dt=2018-06-22, hour=09, msgtype=helper)
OK
Time taken: 4.966 seconds
```
这样就可以在新的hive中执行：
```
select count(*) from hm2.helper;
```
**只导出某一个分区**
```
导出数据
export table hm2.helper partition(dt='2017-12-16') to '/tmp/export/helper_2017-12-16' ;
数据复制
hadoop distcp hdfs://dc1.xx.com:8020/tmp/export/ hdfs://dc2.xx.com:8020/tmp/export
数据导入
import table hm2.helper partition(dt='2017-12-16') from '/tmp/export/helper_2017-12-16'
```
与load data [local] inpath path path2 剪切数据不同，import命令其实是从目标```/tmp/export/hm2/helper```复制到```/user/hive/warehouse/hm2.db/helper```，这时候可以把/tmp/export/hm2/helper目录删掉了。

==可以使用hive export/import 进行hive数据的批量迁移，本实验测试了text，orc，parquet，分区表，并测试了不同版本的导入导出。理论上hive导入导出的数据迁移不受版本，数据格式以及表的限制，可以得出结论可以适应hive export/import进行任何hive数据的迁移==

参考链接：https://blog.csdn.net/u9999/article/details/78830818 

---

# hbase数据迁移
HBase数据迁移是很常见的操作，目前业界主要的迁移方式主要分为以下几类：
![distcp](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/2021/06/09/00:00:38-distcp.png)

从上面图中可看出，目前的方案主要有四类，Hadoop层有一类，HBase层有三类。实际中用了hbase层的Export / Import方法，这里介绍一下。
## Export/Import方式
**源(测试)集群每个节点可以识别目标集群每个节点**
- 源集群hbase执行
```
hbase org.apache.hadoop.hbase.mapreduce.Export 'hm2:test' hdfs://qcloud-hadoop02:9000/tmp/hbase_export/test
```
**注意**：这里路径需要带hdfs://nameser/path ，否则就export 到本地了，下同。
- 目标集群hbase执行
```
hbase org.apache.hadoop.hbase.mapreduce.Import 'hm2:test' hdfs://qcloud-hadoop02:9000/tmp/hbase_export/test
```
**或者**

目标集群每个节点可以识别源(测试)集群每个节点
- 源集群hbase执行
```
hbase org.apache.hadoop.hbase.mapreduce.Export 'hm2:test' hdfs://qcloud-test-hadoop01:9000/tmp/hbase_export/test
```
- 目标集群hbase执行
```
hbase org.apache.hadoop.hbase.mapreduce.Import 'hm2:test' hdfs://qcloud-test-hadoop01:9000/tmp/hbase_export/test
```

## 同步元数据
因为分区信息发生了改变，元信息没有同步。

数据导入到指定的文件夹之后，修复分区和表的元信息，（没有使用rbuy的各种脚本，0.9之后就D了，）
```
hbase hbck -fixTableOrphans 'hm2:test'
hbase hbck -fixMeta 'hm2:test'
hbase hbck -fixAssignments 'hm2:test'
hbase hbck -repair 'hm2:test'
```

## 总结
上文把HBase数据迁移过程中常用的一些方法作了一个大概介绍，总结起来就四点：

- **DistCp**: 文件层的数据同步，也是我们常用的
- **CopyTable**: 这个涉及对原表数据Scan，然后直接Put到目标表，效率较低
- **Export/Import**: 类似CopyTable, Scan出数据放到文件，再把文件传输到目标集群作Import
- **Snapshot**: 比较常用 ， 应用灵活，采用快照技术，效率比较高

具体应用时，要结合自身表的特性，考虑数据规模、数据读写方式、实时数据&离线数据等方面，再选择使用哪种。

#### 资料
https://www.cnblogs.com/felixzh/p/5920153.html
http://hadoop.apache.org/docs/r1.0.4/cn/quickstart.html