一个`snapshot`（快照）是一个全部文件系统、或者某个目录在某一时刻的镜像，使用`vmware`类似软件的同学指定，快照可以为虚拟机保存某个状态，如果做了更改，或者系统被折腾坏，还有个复原的操作。

**快照应用在如下场景中：**

- 防止用户的错误操作；
- 备份：给hdfs目录做快照，然后复制快照里面的文件到备份；
- 试验/测试
- 灾难恢复

## HDFS的快照操作

这里测试的`Hadoop3.0`版本，也是我的线上版本。

1. **开启快照**

```bash
hdfs dfsadmin -allowSnapshot /data/test/test_snapshot

[hdfs@cdh-003 ~]$ hdfs dfs -ls /data/test/test_snapshot
-rw-r--r--   2 hdfs supergroup         88 2021-10-08 16:59 /data/test/test_snapshot/ods_user1.txt
```

这里开启目录`/data/test/test_snapshot`快照功能，该目录当前有一个文件`ods_user1.txt`。

2. **创建快照**

给目录`/data/test/test_snapshot`创建了一个快照，名为`backup01`。

```bash
hdfs dfs -createSnapshot /data/test/test_snapshot backup01

[hdfs@cdh-003 ~]$ hdfs dfs -ls /data/test/test_snapshot/.snapshot/backup01
Found 1 items
-rw-r--r--   2 hdfs supergroup         88 2021-10-08 16:59 /data/test/test_snapshot/.snapshot/backup01/ods_user1.txt
```

可以看到实际上是在开启快照的目录下新建了一个文件夹`.snapshot`，下面有个目录`backup01`，里面有一个文件，相当于复制了一份文件。

3. **查看快照**

```bash
hdfs lsSnapshottableDir
```

再上传一个文件

```bash
hdfs dfs -put data/urls.txt /data/test/test_snapshot
```

再创建一个快照

```bash
 hdfs dfs -createSnapshot /data/test/test_snapshot backup02
```

这里又新建了一个快照`backup02`，`.snapshot`下面有两个目录，分别是`backup01`和`backup02`。

```bash
[hdfs@cdh-003 ~]$ hdfs dfs -ls /data/test/test_snapshot/.snapshot
Found 3 items
drwxr-xr-x   - hdfs supergroup          0 2021-10-08 17:04 /data/test/test_snapshot/.snapshot/backup01
drwxr-xr-x   - hdfs supergroup          0 2021-10-08 17:12 /data/test/test_snapshot/.snapshot/backup02
```

4. **对比快照**

因为后来又上传了一个文件，所以快照`backup02`下面有2个文件了，比`backup01`多一个文件`urls.txt`。

```bash
[hdfs@cdh-003 ~]$ hdfs snapshotDiff /data/test/test_snapshot backup01 backup02
Difference between snapshot backup01 and snapshot backup02 under directory /data/test/test_snapshot:
M	.

+	./urls.txt
```

5. **恢复快照**
   如果不小心把文件删除了，这个文件就被移动到回收站（如果开启了回收站）,如果回收站到期清理了或者没有开启回收站，这个时候快照的作用就发挥出来了，直接`cp`过去。

```bash
hdfs dfs -cp /data/test/test_snapshot/.snapshot/backup01/ods_user1.txt /data/test/test_snapshot
```
**猜你喜欢**<br>
[mysql自增id用完了怎么办？](https://mp.weixin.qq.com/s/U22DumaYlBjhdiZ262Asrg)<br>
[Hadoop 数据迁移用法详解](https://mp.weixin.qq.com/s/L8k0lO_ZbQy7G_46eshnCw)<br>
[Hbase修复工具Hbck](https://mp.weixin.qq.com/s/L2Nvi0HSCbG8pH-DK0cG1Q)<br>
[数仓建模分层理论](https://mp.weixin.qq.com/s/8rpDyo41Kr4r_2wp5hirVA)<br>
[一文搞懂Hive的数据存储与压缩](https://mp.weixin.qq.com/s/90MuP3utZx9BlgbwsfDsfw)<br>
[大数据组件重点学习这几个](https://mp.weixin.qq.com/s/4redHF0e7vCWFqv8t20Rjg)
