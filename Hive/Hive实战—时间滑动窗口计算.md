>关注公众号：`大数据技术派`，回复`资料`，领取`1024G`资料。

[TOC]

## 时间滑动计算

今天遇到一个需求大致是这样的，我们有一个业务涉及到用户打卡，用户可以一天多次打卡，我们希望计算出7天内打卡8次以上，且打卡时间分布在4天以上的时间，当然这只是个例子，我们具体解释一下这个需求

1. 用户一天可以打卡多次，所以要求打卡必须分布在4天以上；
2. 7天不是一个自然周，而是某一天和接下来的6天，也就是说时间是是滑动的，窗口大小是7步长是1，说白了就是窗口计算；

其实说到这里你就想到了窗口函数，虽然这是一个窗口；但是`hive`却没有相应的窗口函数可以计算，接下来我们看一下怎么实现这个逻辑



### 外部调用实现时间循环

我们可以先写这样的一个SQL,就计算每个人在特定时间内是否满足我们的条件，我们先计算出每个人每天的打卡次数，例如这里我们的时间限制是'20210701' 到'20210707'

```sql
  select
     b.union_id,to_date(ds,'yyyymmdd') as dt,count(1) as cnt
  from
    ods_la_daily_record_di b
  where
    -- 驱动表的时间限制
    b.ds>='${bizdate}'
    and b.ds<=${bizdate2}'
  group by
    b.union_id,ds
```

然后我们再判断这个时间端内，用户的打卡情况是否满足我们的条件

```sql
select
  union_id,count(1) as 打卡天数, sum(cnt) as 打卡次数
from
(
  select
     b.union_id,ds,count(1) as cnt
  from
    ods_la_daily_record_di b
  where
    -- 驱动表的时间限制
    b.ds>='${bizdate}'
    and b.ds<='${bizdate2}'
  group by
    b.union_id,ds
)
group by
  union_id
having
  -- 时间分布在4天以上
  count(1)>=4
  -- 打卡次数在8次以上
  and sum(cnt)>=8
;
```

![](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/image-20210722182042258.png)



这样我们就算出来我们需要的数据，接下来我们只需要用其他语言调用这个SQL ,传入不同的时间参数就可以了，利用编程语言实现时间的滑动，例如第一次传入'20210701-20210707' 第二次传入'20210702-20210708' 以此传入即可。

虽然可以实现，但是不好，因为我们还需要其他语言的调用，其实我们知道在SQL里面的关联其实就是通过循环实现的，那我们即然能通过循环实现这个需求，我们能不能通过关联实现这个需求呢

### 自关联实现滑动时间窗口

其实我们只要让用户某一天的数据和他接下来的6天的数据关联，然后按照这一天的数据进行汇总然后判断时候满足我们的条件即可，如果满足了条件，那么用户这一天的数据就是满足我们的需求的，也就是说这个用户是满足我们的需求的。

```sql
with tmp as(
   -- 每个人每天打卡的次数
  select
     b.union_id,to_date(ds,'yyyymmdd') as dt,count(1) as cnt
  from
    ods_la_daily_record_di b
  where
    -- 驱动表的时间限制
    b.ds>='${bizdate}'
  group by
    b.union_id,ds
)
select
  union_id
from (
  -- 满足条件的(用户-天)
  select
    a.union_id,a.dt,sum(b.cnt) as 打卡次数,count(1) as 打卡天数
  from
    tmp a
  inner join
   tmp b
  on
    a.union_id=b.union_id
    and DATEDIFF(b.dt,a.dt)>=0
    and DATEDIFF(b.dt,a.dt)<=6
  group by
    a.union_id,a.dt
  having
    -- 次数限制
    sum(b.cnt)>=8
    -- 天数限制
    and count(1)>=4
)group by
  -- 对用户去重
  union_id
;
```

这里有一个问题需要注意一下，那就是我们满足条件`sum(b.cnt)>=8 and count(1)>=4` 的是用户某一天的数据，也就是说我们的维度是`union_id-天`，所以我们需要对这个数据按照用户为度进行去重。

### 扩展基于自然周的的滚动时间窗口计算

我们这里思考一个问题，那就是我们知道很多时候我们的计算其实是围绕着自然周的，虽然我们上面的计算不是自然周，那假设我们如果要求我们的计算是自然周呢，那这个时候我们应该怎么计算呢，其实我们数仓里有一种很表叫做时间维表，我们利用时间维表可以很方便的计算时间相关的东西，如果你没有的话建议去网上找一份，或者自己生成一份，因为使用起来很方便。

![](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/20210722220447.png)

因为这个表的字段很多，这里我们截取了一部分放到这里了，下面我们看一下怎么使用时间维表进行计算。

```sql
select
   UNION_ID,time_weeknum,count(1) as 打卡天数, sum(cnt) as 打卡次数
from(
  select
     b.union_id,ds,count(1) as cnt
  from
    ods_la_daily_record_di b
  where
    -- 驱动表的时间限制
    b.ds>='${bizdate}'
    and b.ds<='${bizdate2}'
  group by
    b.union_id,ds
) a
left join
dim_date_time b
on
  a.ds=b.time_date
group by
  --  周的标识
  UNION_ID,time_weeknum
HAVING
  -- 时间分布在4天以上
  count(1)>=4
  -- 打卡次数在8次以上
  and sum(cnt)>=8
;
```

这里我们就基于每个自然周算出了满足条件的人，当然我们还是要针对用户去重



## 总结

我们看到自关联其实可以达到滑动的效果，当然不仅仅体现在时间上，就像窗口除了时间窗口还是有基于个数的窗口，我们要在遇到类似问题的时候就可以选择这样的解决方案。

时间维表很重要，可以简化我们的计算，如果没有的话，需要创建一个。

**交流群**

加我微信：`ddxygq`，回复`加群`，我拉你进技术交流群。

**猜你喜欢**<br>
[数仓建模—指标体系](https://mp.weixin.qq.com/s/H3vbulk3gavIvV40LrIagA)<br>
[数仓建模—宽表的设计](https://mp.weixin.qq.com/s/Jsi55C4eHE-O69e3JwwIcg)<br>
[Spark SQL知识点与实战](https://mp.weixin.qq.com/s/q4L7hnUpab7rnEwCA5yRUQ)<br>
[Hive计算最大连续登陆天数](https://mp.weixin.qq.com/s/2Z2Y7QsA_eZRblXfYbHjxw)<br>
[Flink计算pv和uv的通用方法](https://mp.weixin.qq.com/s/6nApSSK-xDAwnXp1r2m-ug)