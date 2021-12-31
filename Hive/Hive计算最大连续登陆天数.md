强哥说他发现了财富密码，最近搞了一套股票算法，其中有一点涉及到股票连续涨停天数的计算方法，我们都知道股票周末是不开市的，这里有个断层，需要一点技巧。我问是不是时间序列，他说我瞎扯，我也知道自己是瞎扯。`问他方法，他竟然不告诉我，这么多年的兄弟情谊算个屁`。真当我没他聪明吗，哼！

靠人不如靠自己，我决定连夜研究一下在`Hive`里面计算最大连续天数的计算方法。

## 一、背景

在网站平台类业务需求中用户的「最大登陆天数」，需求比较普遍。

原始数据：

```
u0001 2019-10-10
u0001 2019-10-11
u0001 2019-10-12
u0001 2019-10-14
u0001 2019-10-15
u0001 2019-10-17
u0001 2019-10-18
u0001 2019-10-19
u0001 2019-10-20
u0002 2019-10-20
```



**说明**：数据是简化版，两列分别是user_id,log_in_date。现实情况需要从采集数据经过去重，转换得到以上形式数据。

我们先建表并且将数据导入`Hive`：

```sql
create table test.user_log_1 (user_id string, log_in_date string) row format delimited fields terminated by ' ';

load data local inpath '/var/lib/hadoop-hdfs/data/user_log.txt' into table test.user_log_1 ;
```

查看一下数据：

```
hive> select * from test.user_log_1 ;
OK
u0001	2019-10-10
u0001	2019-10-11
u0001	2019-10-12
u0001	2019-10-14
u0001	2019-10-15
u0001	2019-10-17
u0001	2019-10-18
u0001	2019-10-19
u0001	2019-10-20
u0002	2019-10-20
Time taken: 0.076 seconds, Fetched: 10 row(s)
```



## 二、算法

核心是按访问时间排序，登陆时间列减去排序后的序列号，得到一个日期值，按这个值分组计数即可。

#### 1. 第一步：排序

按照`user_id`分组，并且按照日期`log_in_date`排序：

```sql
select user_id, log_in_date, row_number() over(partition by user_id order by log_in_date) as rank from test.user_log_1;
```

结果：

```
u0001	2019-10-10	1
u0001	2019-10-11	2
u0001	2019-10-12	3
u0001	2019-10-14	4
u0001	2019-10-15	5
u0001	2019-10-17	6
u0001	2019-10-18	7
u0001	2019-10-19	8
u0001	2019-10-20	9

u0002	2019-10-20	1
```

这里可以看出，`u0001`这个用户最大连续登录天数是4天，使用后面计算方法计算后可以验证。

#### 2. 第二步：第二列与第三列做日期差值

可以看出规律，日期小的，行号也小；如果将日期跟行号做差值，连续登录的差值应该是一样的。

```sql
select user_id, date_sub(log_in_date, rank) dts from (select user_id, log_in_date, row_number() over(partition by user_id order by log_in_date) as rank from test.user_log_1)m;
```

结果：

```
u0001	2019-10-09
u0001	2019-10-09
u0001	2019-10-09
u0001	2019-10-10
u0001	2019-10-10
u0001	2019-10-11
u0001	2019-10-11
u0001	2019-10-11
u0001	2019-10-11
u0002	2019-10-19
```

显然可以看出，最大连续连续登录是4次。

#### 3. 第三步:按第二列分组求和

```sql
select user_id, dts, count(1) num from (select user_id, date_sub(log_in_date, rank) dts from (select user_id, log_in_date, row_number() over(partition by user_id order by log_in_date) as rank from test.user_log_1)m)m2 group by user_id, dts;
```

结果：

```
u0001	2019-10-09	3
u0001	2019-10-10	2
u0001	2019-10-11	4
u0002	2019-10-19	1
```

#### 4. 第四步：求最大次数

已经算出了，每个用户连续登录天数序列，接下取每个用户最大登录天数最大值即可：

```sql
select user_id, max(num) from (select user_id, dts, count(1) num from (select user_id, date_sub(log_in_date, rank) dts from (select user_id, log_in_date, row_number() over(partition by user_id order by log_in_date) as rank from test.user_log_1)m)m2 group by user_id, dts)m3 group by user_id;
```

结果跟我们的预期是一致的，用户`u0001`最大登录天数是4。

```
u0001	4
u0002	1
```



## 三、扩展（股票最大涨停天数）

我们知道股票市场，比如咱们的A股，周末是不开盘的，那么一只股票如果上周五涨停，本周一接着涨停，这算是连续2天涨停，使用上面这种方法是不行的，使用`lead`函数试试：

```sql
select user_id, log_in_date, lead(log_in_date) over(partition by user_id order by log_in_date) end_date from test.user_log_1;
```

结果

```
u0001	2019-10-10	2019-10-11
u0001	2019-10-11	2019-10-12
u0001	2019-10-12	2019-10-14
u0001	2019-10-14	2019-10-15
u0001	2019-10-15	2019-10-17
u0001	2019-10-17	2019-10-18
u0001	2019-10-18	2019-10-19
u0001	2019-10-19	2019-10-20
u0001	2019-10-20	NULL
u0002	2019-10-20	NULL
```

哈哈，是不是有思路了。

**思路**：上面结果一共有3列，第一列是`uid`，通过`lead`函数，后面两列都是日期，那么两列日期都取值`周一`到`周五`之间，也就是说数据里面只有工作日日期，没有周末的数据，可以提前过滤使得数据满足，既然要连续，那么：

1. 如果第三列的日期，减去第二列的日期，`差值等于1`，显然是连续的；
2. 如果第三列的日期，减去第二列的日期，`差值等于3`，但是第三列日期是星期一，那么也算是连续了；

以上两种条件综合，就能计算出股票的`最大连续涨停天数`了，你学废了吗。

**猜你喜欢**<br>
[HDFS的快照讲解](https://mp.weixin.qq.com/s/ooYIcHQ5V9x2fh3G7ZhCxg)<br>
[Hadoop 数据迁移用法详解](https://mp.weixin.qq.com/s/L8k0lO_ZbQy7G_46eshnCw)<br>
[Hbase修复工具Hbck](https://mp.weixin.qq.com/s/L2Nvi0HSCbG8pH-DK0cG1Q)<br>
[数仓建模分层理论](https://mp.weixin.qq.com/s/8rpDyo41Kr4r_2wp5hirVA)<br>
[一文搞懂Hive的数据存储与压缩](https://mp.weixin.qq.com/s/90MuP3utZx9BlgbwsfDsfw)<br>
[大数据组件重点学习这几个](https://mp.weixin.qq.com/s/4redHF0e7vCWFqv8t20Rjg)
