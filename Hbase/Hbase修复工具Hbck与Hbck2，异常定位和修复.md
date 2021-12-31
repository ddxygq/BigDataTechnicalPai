因为前面Hbase2集群出现过一次故障，当时花了一个周末才修好，就去了解整理了一些hbase故障的，事故现场可以看前面写的一篇：[Hbase集群挂掉的一次惊险经历](https://mp.weixin.qq.com/s/YAEiMuC61W7HgBcckiaOJA)
## 一. HBCK一致性
一致性是指Region在meta中的meta表信息、在线Regionserver的Region信息和hdfs的Regioninfo的Region信息的一致。
![hbck](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/2021/09/25/22:42:00-hbck.jpg)

## 二. HBCK2与hbck1
HBCK2是后继hbck，该修复工具，随HBase的-1.x的（AKA hbck1）。使用HBCK2代替 hbck1对 hbase-2.x 集群进行修复。hbck1不应针对 hbase-2.x 安装运行。它可能会造成损害。虽然hbck1仍然捆绑在 hbase-2.x 中——以尽量减少意外——但它已被**弃用**，将在hbase-3.x 中删除。它的写入工具 ( -fix) 已被删除。它可以报告 hbase-2.x 集群的状态，但它的评估将是不准确的，因为它不了解 hbase-2.x 的内部工作原理。

我这里是hbase版本是`2.0.0-cdh6.0.1`，`hbase hbck -h`显示的是：
```bash
-----------------------------------------------------------------------
NOTE: As of HBase version 2.0, the hbck tool is significantly changed.
In general, all Read-Only options are supported and can be be used
safely. Most -fix/ -repair options are NOT supported. Please see usage
below for details on which options are not supported.
-----------------------------------------------------------------------
```
hbase2.0*是不支持hbck的，很多只读命令还可以执行，修复命令完全不能执行，hbase2只能自己去官网下载，自己编译修复工具，也不知道`hbase`团队咋想滴，整合在shell命令中多好，还要使用者自己去编译，随着版本升级，越来越多的公司将从1.x升级到2.x。

**NOTE: Following options are NOT supported as of HBase version 2.0+.**
```bash

  UNSUPPORTED Metadata Repair options: (expert features, use with caution!)
   -fix              Try to fix region assignments.  This is for backwards compatiblity
   -fixAssignments   Try to fix region assignments.  Replaces the old -fix
   -fixMeta          Try to fix meta problems.  This assumes HDFS region info is good.
   -fixHdfsHoles     Try to fix region holes in hdfs.
   -fixHdfsOrphans   Try to fix region dirs with no .regioninfo file in hdfs
   -fixTableOrphans  Try to fix table dirs with no .tableinfo file in hdfs (online mode only)
   -fixHdfsOverlaps  Try to fix region overlaps in hdfs.
   -maxMerge <n>     When fixing region overlaps, allow at most <n> regions to merge. (n=5 by default)
   -sidelineBigOverlaps  When fixing region overlaps, allow to sideline big overlaps
   -maxOverlapsToSideline <n>  When fixing region overlaps, allow at most <n> regions to sideline per group. (n=2 by default)
   -fixSplitParents  Try to force offline split parents to be online.
   -removeParents    Try to offline and sideline lingering parents and keep daughter regions.
   -fixEmptyMetaCells  Try to fix hbase:meta entries not referencing any region (empty REGIONINFO_QUALIFIER rows)

  UNSUPPORTED Metadata Repair shortcuts
   -repair           Shortcut for -fixAssignments -fixMeta -fixHdfsHoles -fixHdfsOrphans -fixHdfsOverlaps -fixVersionFile -sidelineBigOverlaps -fixReferenceFiles-fixHFileLinks
   -repairHoles      Shortcut for -fixAssignments -fixMeta -fixHdfsHoles
```
在hbase2中，hbck的命令是不支持修复的，需要使用hbck2命令，后面会介绍。

## 三. Hbck 一致性的检查和修复命令
**一致性检查命令**
```bash
hbase hbck <-details> <表名>
```
**一致性修复**
```bash
hbase hbck <-fixMeta> ,<-fixAssignments> <表名>
```
命令详解 
```bash
-fixMeta：Try to fix meta problems.  This assumes HDFS region info is good.
```
主要以hdfs为准进行修复，hdfs存在则添加到meta中，不存在删除meta对应region。
```bash
-fixAssignments:Try to fix region assignments.  Replaces the old -fix
```
不同情况，动作不一样，包括下线、关闭和重新上线

## 四. Hbck异常定位和修复
region在meta、regionserver和hdfs三者都有哪些不一致？怎么修复？可以根据下面的异常清单进行异常定位和修复:
| 不一致                                                       | 异常信息                                                     | 修复                           |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------ |
|**第一种情况** | **Region Is Not In Hbase:Meta**|   |
| Region信息在meta数据和hdfs都不存在，但是却被部署到Regionserver。 | errors.reportError(ERROR_CODE.NOT_IN_META_HDFS, "Region "<br/>    + descriptiveName + ", key=" + key + ", not on HDFS or in hbase:meta but " +<br/>    "deployed on " + Joiner.on(", ").join(hbi.deployedOn)); | FixAssignments                 |
| Region在meta数据表不存在，也没有被部署到Regionserver，但是数据在hdfs上。 | errors.reportError(ERROR_CODE.NOT_IN_META_OR_DEPLOYED, "Region "<br/>    + descriptiveName + " on HDFS, but not listed in hbase:meta " +<br/>    "or deployed on any Region server" | - FixMeta<br>- FixAssignments  |
| Region在meta数据表不存在，但是在Regionserver部署，数据在hdfs上。 | errors.reportError(ERROR_CODE.NOT_IN_META, "Region " + descriptiveName<br/>    + " not in META, but deployed on " + Joiner.on(", ").join(hbi.deployedOn)); | 1.FixMeta<br/>2.FixAssignments |
|   **第二种情况**  | **Region Is In Hbase:Meta** |    |
| Region只存在meta中，但在hdfs和rs上都不存在                   | errors.reportError(ERROR_CODE.NOT_IN_HDFS_OR_DEPLOYED, "Region "<br/>    + descriptiveName + " found in META, but not in HDFS "<br/>    + "or deployed on any Region server.") | FixMeta                        |
| Region在meta表和Regionserver中存在，但是在hdfs不存在。       | errors.reportError(ERROR_CODE.NOT_IN_HDFS, "Region " + descriptiveName<br/>    + " found in META, but not in HDFS, " +<br/>    "and deployed on " + Joiner.on(", ").join(hbi.deployedOn)); | 1.FixAssignments<br/>2.FixMeta |
| Region在meta表和hdfs中存在，且Region所在表没有处于disable状态，但是没有部署。 | errors.reportError(ERROR_CODE.**NOT_DEPLOYED**, "Region " + descriptiveName<br/>  \+ " not deployed on any Region server."); | FixAssignments                 |
| Region处于disabling或disabled                                | errors.reportError(ERROR_CODE.SHOULD_NOT_BE_DEPLOYED,<br/>    "Region " + descriptiveName + " should not be deployed according " +<br/>    "to META, but is deployed on " + Joiner.on(", ").join(hbi.deployedOn)); | FixAssignments                 |
| Region多分配                                                 | errors.reportError(ERROR_CODE.MULTI_DEPLOYED, "Region " + descriptiveName<br/>    + " is listed in hbase:meta on Region server " + hbi.metaEntry.RegionServer<br/>    + " but is multiply assigned to Region servers " +<br/>    Joiner.on(", ").join(hbi.deployedOn)); | FixAssignments                 |
| Region在meta表的Regionserver信息与实际部署的Regionserver不一致。 | errors.reportError(ERROR_CODE.SERVER_DOES_NOT_MATCH_META, "Region "<br/>    + descriptiveName + " listed in hbase:meta on Region server " +<br/>    hbi.metaEntry.RegionServer + " but found on Region server " +<br/>    hbi.deployedOn.get(0)); | FixAssignments                 |
| 父region在meta和hdfs存在，且处于切分状态，但子region的信息在meta信息缺失。 | errors.reportError(ERROR_CODE.LINGERING_SPLIT_PARENT, "Region "<br/>+ descriptiveName + " is a split parent in META, in HDFS, "<br/>+ "and not deployed on any region server. This could be transient, "<br/>+ "consider to run the catalog janitor first!"); | fixSplitParents                |

## 五. Hbck2命令
HBCK是HBase1.x中的命令，到了HBase2.x中，HBCK命令不适用，且它的写功能（-fix）已删除，它虽然还可以报告HBase2.x集群的状态，但是由于它不了解HBase2.x集群内部的工作原理，因此其评估将不准确。因此，如果你正在使用HBase2.x，那么对HBCK2应该需要了解一些，即使你不经常用到。

#### 1. 获取HBCK2
HBCK2已经被剥离出HBase成为了一个单独的项目，如果你想要使用这个工具，需要根据自己HBase的版本，编译源码。

其GitHub地址为：https://github.com/apache/hbase-operator-tools.git
![hbck2](https://kingcall.oss-cn-hangzhou.aliyuncs.com/blog/img/2021/09/25/23:13:59-hbck2.jpg)

在`pom`中将hbase版本换成自己实际的`hbase2.x`版本，项目根目录下运行打包命令：
```bash
mvn clean install -DskipTests
```
打包完成后，是有多个jar包的，将自己需要的hbck2取出来`hbase-operator-tools/hbase-hbck2/target/hbase-hbck2-1.0.0-SNAPSHOT.jar`。
#### 2. 使用Hback2
HBCK2其依赖项的最简单方法是通过脚本启动`$HBASE_HOME/bin/hbase`。该bin/hbase脚本本身就提到了hbck-hbck帮助输出中列出了一个选项。默认情况下， running将运行bin/hbase hbck内置的hbck1工具。要运行HBCK2，您需要使用以下选项指向构建的`HBCK2 jar -j`：
```bash
${HBASE_HOME}/bin/hbase --config /etc/hbase-conf hbck -j ~/hbase-operator-tools/hbase-hbck2/target/hbase-hbck2-1.0.0-SNAPSHOT.jar
```
上面/etc/hbase-conf的位置是部署的配置所在的位置，上面没有传递选项或参数的命令将转储出HBCK2帮助：
```bash
usage: HBCK2 [OPTIONS] COMMAND <ARGS>
Options:
 -d,--debug                                       run with debug output
 -h,--help                                        output this help message
 -p,--hbase.zookeeper.property.clientPort <arg>   port of hbase ensemble
 -q,--hbase.zookeeper.quorum <arg>                hbase ensemble
 -s,--skip                                        skip hbase version check
                                                  (PleaseHoldException)
 -v,--version                                     this hbck2 version
 -z,--zookeeper.znode.parent <arg>                parent znode of hbase
                                                  ensemble
Command:
 addFsRegionsMissingInMeta <NAMESPACE|NAMESPACE:TABLENAME>...
   Options:
    -d,--force_disable aborts fix for table if disable fails.
   To be used when regions missing from hbase:meta but directories
   are present still in HDFS. Can happen if user has run _hbck1_
   'OfflineMetaRepair' against an hbase-2.x cluster. Needs hbase:meta
   to be online. For each table name passed as parameter, performs diff
   between regions available in hbase:meta and region dirs on HDFS.
   Then for dirs with no hbase:meta matches, it reads the 'regioninfo'
   metadata file and re-creates given region in hbase:meta. Regions are
   re-created in 'CLOSED' state in the hbase:meta table, but not in the
   Masters' cache, and they are not assigned either. To get these
   regions online, run the HBCK2 'assigns'command printed when this
   command-run completes.
   NOTE: If using hbase releases older than 2.3.0, a rolling restart of
   HMasters is needed prior to executing the set of 'assigns' output.
   An example adding missing regions for tables 'tbl_1' in the default
   namespace, 'tbl_2' in namespace 'n1' and for all tables from
   namespace 'n2':
     $ HBCK2 addFsRegionsMissingInMeta default:tbl_1 n1:tbl_2 n2
   Returns HBCK2  an 'assigns' command with all re-inserted regions.
   SEE ALSO: reportMissingRegionsInMeta
   SEE ALSO: fixMeta

 assigns [OPTIONS] <ENCODED_REGIONNAME/INPUTFILES_FOR_REGIONNAMES>...
   Options:
    -o,--override  override ownership by another procedure
    -i,--inputFiles  take one or more encoded region names
   A 'raw' assign that can be used even during Master initialization (if
   the -skip flag is specified). Skirts Coprocessors. Pass one or more
   encoded region names. 1588230740 is the hard-coded name for the
   hbase:meta region and de00010733901a05f5a2a3a382e27dd4 is an example of
   what a user-space encoded region name looks like. For example:
     $ HBCK2 assigns 1588230740 de00010733901a05f5a2a3a382e27dd4
   Returns the pid(s) of the created AssignProcedure(s) or -1 if none.
   If -i or --inputFiles is specified, pass one or more input file names.
   Each file contains encoded region names, one per line. For example:
     $ HBCK2 assigns -i fileName1 fileName2
 bypass [OPTIONS] <PID>...
   Options:
    -o,--override   override if procedure is running/stuck
    -r,--recursive  bypass parent and its children. SLOW! EXPENSIVE!
    -w,--lockWait   milliseconds to wait before giving up; default=1
   Pass one (or more) procedure 'pid's to skip to procedure finish. Parent
   of bypassed procedure will also be skipped to the finish. Entities will
   be left in an inconsistent state and will require manual fixup. May
   need Master restart to clear locks still held. Bypass fails if
   procedure has children. Add 'recursive' if all you have is a parent pid
   to finish parent and children. This is SLOW, and dangerous so use
   selectively. Does not always work.

 extraRegionsInMeta <NAMESPACE|NAMESPACE:TABLENAME>...
   Options:
    -f, --fix    fix meta by removing all extra regions found.
   Reports regions present on hbase:meta, but with no related
   directories on the file system. Needs hbase:meta to be online.
   For each table name passed as parameter, performs diff
   between regions available in hbase:meta and region dirs on the given
   file system. Extra regions would get deleted from Meta
   if passed the --fix option.
   NOTE: Before deciding on use the "--fix" option, it's worth check if
   reported extra regions are overlapping with existing valid regions.
   If so, then "extraRegionsInMeta --fix" is indeed the optimal solution.
   Otherwise, "assigns" command is the simpler solution, as it recreates
   regions dirs in the filesystem, if not existing.
   An example triggering extra regions report for tables 'table_1'
   and 'table_2', under default namespace:
     $ HBCK2 extraRegionsInMeta default:table_1 default:table_2
   An example triggering extra regions report for table 'table_1'
   under default namespace, and for all tables from namespace 'ns1':
     $ HBCK2 extraRegionsInMeta default:table_1 ns1
   Returns list of extra regions for each table passed as parameter, or
   for each table on namespaces specified as parameter.

 filesystem [OPTIONS] [<TABLENAME>...]
   Options:
    -f, --fix    sideline corrupt hfiles, bad links, and references.
   Report on corrupt hfiles, references, broken links, and integrity.
   Pass '--fix' to sideline corrupt files and links. '--fix' does NOT
   fix integrity issues; i.e. 'holes' or 'orphan' regions. Pass one or
   more tablenames to narrow checkup. Default checks all tables and
   restores 'hbase.version' if missing. Interacts with the filesystem
   only! Modified regions need to be reopened to pick-up changes.

 fixMeta
   Do a server-side fix of bad or inconsistent state in hbase:meta.
   Available in hbase 2.2.1/2.1.6 or newer versions. Master UI has
   matching, new 'HBCK Report' tab that dumps reports generated by
   most recent run of _catalogjanitor_ and a new 'HBCK Chore'. It
   is critical that hbase:meta first be made healthy before making
   any other repairs. Fixes 'holes', 'overlaps', etc., creating
   (empty) region directories in HDFS to match regions added to
   hbase:meta. Command is NOT the same as the old _hbck1_ command
   named similarily. Works against the reports generated by the last
   catalog_janitor and hbck chore runs. If nothing to fix, run is a
   noop. Otherwise, if 'HBCK Report' UI reports problems, a run of
   fixMeta will clear up hbase:meta issues. See 'HBase HBCK' UI
   for how to generate new report.
   SEE ALSO: reportMissingRegionsInMeta

 generateMissingTableDescriptorFile <TABLENAME>
   Trying to fix an orphan table by generating a missing table descriptor
   file. This command will have no effect if the table folder is missing
   or if the .tableinfo is present (we don't override existing table
   descriptors). This command will first check it the TableDescriptor is
   cached in HBase Master in which case it will recover the .tableinfo
   accordingly. If TableDescriptor is not cached in master then it will
   create a default .tableinfo file with the following items:
     - the table name
     - the column family list determined based on the file system
     - the default properties for both TableDescriptor and
       ColumnFamilyDescriptors
   If the .tableinfo file was generated using default parameters then
   make sure you check the table / column family properties later (and
   change them if needed).
   This method does not change anything in HBase, only writes the new
   .tableinfo file to the file system. Orphan tables can cause e.g.
   ServerCrashProcedures to stuck, you might need to fix these still
   after you generated the missing table info files.

 replication [OPTIONS] [<TABLENAME>...]
   Options:
    -f, --fix    fix any replication issues found.
   Looks for undeleted replication queues and deletes them if passed the
   '--fix' option. Pass a table name to check for replication barrier and
   purge if '--fix'.

 reportMissingRegionsInMeta <NAMESPACE|NAMESPACE:TABLENAME>...
   To be used when regions missing from hbase:meta but directories
   are present still in HDFS. Can happen if user has run _hbck1_
   'OfflineMetaRepair' against an hbase-2.x cluster. This is a CHECK only
   method, designed for reporting purposes and doesn't perform any
   fixes, providing a view of which regions (if any) would get re-added
   to hbase:meta, grouped by respective table/namespace. To effectively
   re-add regions in meta, run addFsRegionsMissingInMeta.
   This command needs hbase:meta to be online. For each namespace/table
   passed as parameter, it performs a diff between regions available in
   hbase:meta against existing regions dirs on HDFS. Region dirs with no
   matches are printed grouped under its related table name. Tables with
   no missing regions will show a 'no missing regions' message. If no
   namespace or table is specified, it will verify all existing regions.
   It accepts a combination of multiple namespace and tables. Table names
   should include the namespace portion, even for tables in the default
   namespace, otherwise it will assume as a namespace value.
   An example triggering missing regions report for tables 'table_1'
   and 'table_2', under default namespace:
     $ HBCK2 reportMissingRegionsInMeta default:table_1 default:table_2
   An example triggering missing regions report for table 'table_1'
   under default namespace, and for all tables from namespace 'ns1':
     $ HBCK2 reportMissingRegionsInMeta default:table_1 ns1
   Returns list of missing regions for each table passed as parameter, or
   for each table on namespaces specified as parameter.

 setRegionState <ENCODED_REGIONNAME> <STATE>
   Possible region states:
    OFFLINE, OPENING, OPEN, CLOSING, CLOSED, SPLITTING, SPLIT,
    FAILED_OPEN, FAILED_CLOSE, MERGING, MERGED, SPLITTING_NEW,
    MERGING_NEW, ABNORMALLY_CLOSED
   WARNING: This is a very risky option intended for use as last resort.
   Example scenarios include unassigns/assigns that can't move forward
   because region is in an inconsistent state in 'hbase:meta'. For
   example, the 'unassigns' command can only proceed if passed a region
   in one of the following states: SPLITTING|SPLIT|MERGING|OPEN|CLOSING
   Before manually setting a region state with this command, please
   certify that this region is not being handled by a running procedure,
   such as 'assign' or 'split'. You can get a view of running procedures
   in the hbase shell using the 'list_procedures' command. An example
   setting region 'de00010733901a05f5a2a3a382e27dd4' to CLOSING:
     $ HBCK2 setRegionState de00010733901a05f5a2a3a382e27dd4 CLOSING
   Returns "0" if region state changed and "1" otherwise.

 setTableState <TABLENAME> <STATE>
   Possible table states: ENABLED, DISABLED, DISABLING, ENABLING
   To read current table state, in the hbase shell run:
     hbase> get 'hbase:meta', '<TABLENAME>', 'table:state'
   A value of \x08\x00 == ENABLED, \x08\x01 == DISABLED, etc.
   Can also run a 'describe "<TABLENAME>"' at the shell prompt.
   An example making table name 'user' ENABLED:
     $ HBCK2 setTableState users ENABLED
   Returns whatever the previous table state was.

 scheduleRecoveries <SERVERNAME>...
   Schedule ServerCrashProcedure(SCP) for list of RegionServers. Format
   server name as '<HOSTNAME>,<PORT>,<STARTCODE>' (See HBase UI/logs).
   Example using RegionServer 'a.example.org,29100,1540348649479':
     $ HBCK2 scheduleRecoveries a.example.org,29100,1540348649479
   Returns the pid(s) of the created ServerCrashProcedure(s) or -1 if
   no procedure created (see master logs for why not).
   Command support added in hbase versions 2.0.3, 2.1.2, 2.2.0 or newer.

 unassigns <ENCODED_REGIONNAME>...
   Options:
    -o,--override  override ownership by another procedure
   A 'raw' unassign that can be used even during Master initialization
   (if the -skip flag is specified). Skirts Coprocessors. Pass one or
   more encoded region names. 1588230740 is the hard-coded name for the
   hbase:meta region and de00010733901a05f5a2a3a382e27dd4 is an example
   of what a userspace encoded region name looks like. For example:
     $ HBCK2 unassign 1588230740 de00010733901a05f5a2a3a382e27dd4
   Returns the pid(s) of the created UnassignProcedure(s) or -1 if none.

   SEE ALSO, org.apache.hbase.hbck1.OfflineMetaRepair, the offline
   hbase:meta tool. See the HBCK2 README for how to use.
```
这样就看到熟悉的命令：`assigns`, `bypass`, `extraRegionsInMeta`,`fixMeta`。
这些都是官方文档的内容，写的很清楚了，有时间可以慢慢看下。
`https://github.com/apache/hbase-operator-tools/tree/master/hbase-hbck2`

**猜你喜欢**<br>
[Hadoop3数据容错技术（纠删码）](https://mp.weixin.qq.com/s/mznZZo-vqjYdFXN2z5DpPA)<br>
[Hadoop 数据迁移用法详解](https://mp.weixin.qq.com/s/L8k0lO_ZbQy7G_46eshnCw)<br>
[Flink实时计算topN热榜](https://mp.weixin.qq.com/s/9K3oclvWDt0y14DIkDmQrw)<br>
[数仓建模分层理论](https://mp.weixin.qq.com/s/8rpDyo41Kr4r_2wp5hirVA)<br>
[一文搞懂Hive的数据存储与压缩](https://mp.weixin.qq.com/s/90MuP3utZx9BlgbwsfDsfw)<br>
[大数据组件重点学习这几个](https://mp.weixin.qq.com/s/4redHF0e7vCWFqv8t20Rjg)
