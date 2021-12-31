>关注公众号：`大数据技术派`，回复`资料`，领取`1024G`资料。

有些时候需要我们去统计某一个hbase表的行数，由于hbase本身不支持SQL语言，只能通过其他方式实现。

![](http://qiniu.ikeguang.com/image/2021/06/15/21:26:15-%E6%9C%AA%E5%91%BD%E5%90%8D%E6%96%87%E4%BB%B6.jpg)

可以通过一下几种方式实现hbase表的行数统计工作:

这里有一张hbase表test:test：
```bash
hbase(main):009:0> scan 'test:test'
ROW                              COLUMN+CELL                                                                                
 1                               column=info:name, timestamp=1590221288866, value=tom                                       
 2                               column=info:name, timestamp=1590221288866, value=jack                                      
 3                               column=info:name, timestamp=1590221288866, value=alice                                     
3 row(s) in 0.0700 seconds
```

## 1.count命令

最直接的方式是在hbase shell中执行count的命令可以统计行数。
```
hbase(main):010:0> count 'test:test'
3 row(s) in 0.0170 seconds

=> 3
```
其中，INTERVAL为统计的行数间隔，默认为1000，CACHE为统计的数据缓存。这种方式效率很低，如果表行数很大的话不建议采用这种方式。

## 2. 调用Mapreduce
```
$HBASE_HOME/bin/hbase   org.apache.hadoop.hbase.mapreduce.RowCounter ‘tablename’
```
这种方式效率比上一种要搞很多，调用的hbase jar中自带的统计行数的类。
```bash
hbase  org.apache.hadoop.hbase.mapreduce.RowCounter 'test:test'
```
中间走的是一个MR任务，可以看一下控制台的输出信息：
```bash
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/home/hadoop/hbase/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
2020-05-23 16:40:57,546 WARN  [main] util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
2020-05-23 16:41:00,256 WARN  [main] mapreduce.TableMapReduceUtil: The hbase-prefix-tree module jar containing PrefixTreeCodec is not present.  Continuing without it.
2020-05-23 16:41:00,784 INFO  [main] client.RMProxy: Connecting to ResourceManager at master1.hadoop/192.168.112.210:8032
2020-05-23 16:41:13,932 INFO  [main] zookeeper.RecoverableZooKeeper: Process identifier=hconnection-0x56ace400 connecting to ZooKeeper ensemble=master1.hadoop:2181,slave2.hadoop:2181,slave3.hadoop:2181
2020-05-23 16:41:13,963 INFO  [main] zookeeper.ZooKeeper: Client environment:zookeeper.version=3.4.6-1569965, built on 02/20/2014 09:09 GMT
2020-05-23 16:41:13,964 INFO  [main] zookeeper.ZooKeeper: Client environment:host.name=slave3.hadoop
2020-05-23 16:41:13,964 INFO  [main] zookeeper.ZooKeeper: Client environment:java.version=1.8.0_73
2020-05-23 16:41:13,964 INFO  [main] zookeeper.ZooKeeper: Client environment:java.vendor=Oracle Corporation
2020-05-23 16:41:13,964 INFO  [main] zookeeper.ZooKeeper: Client environment:java.home=/home/hadoop/jdk1.8.0_73/jre
2020-05-23 16:41:13,964 INFO  [main] zookeeper.ZooKeeper: Client environment:java.class.path=/home/hadoop/hbase/conf:/home/hadoop/jdk1.8.0_73/lib/tools.jar:/home/hadoop/hbase:/home/hadoop/hbase/lib/activation-1.1.jar:/home/hadoop/hbase/lib/antisamy-1.5.3.jar:/home/hadoop/hbase/lib/aopalliance-1.0.jar:/home/hadoop/hbase/lib/apacheds-i18n-2.0.0-M15.jar:/home/hadoop/hbase/lib/apacheds-kerberos-codec-2.0.0-M15.jar:/home/hadoop/hbase/lib/api-asn1-api-1.0.0-M20.jar:/home/hadoop/hbase/lib/api-util-1.0.0-M20.jar:/home/hadoop/hbase/lib/asm-3.1.jar:/home/hadoop/hbase/lib/avro-1.7.4.jar:/home/hadoop/hbase/lib/batik-css-1.8.jar:/home/hadoop/hbase/lib/batik-ext-1.8.jar:/home/hadoop/hbase/lib/batik-util-1.8.jar:/home/hadoop/hbase/lib/bsh-core-2.0b4.jar:/home/hadoop/hbase/lib/commons-beanutils-1.7.0.jar:/home/hadoop/hbase/lib/commons-beanutils-core-1.8.3.jar:/home/hadoop/hbase/lib/commons-cli-1.2.jar:/home/hadoop/hbase/lib/commons-codec-1.9.jar:/home/hadoop/hbase/lib/commons-collections-3.2.2.jar:/home/hadoop/hbase/lib/commons-compress-1.4.1.jar:/home/hadoop/hbase/lib/commons-configuration-1.6.jar:/home/hadoop/hbase/lib/commons-daemon-1.0.13.jar:/home/hadoop/hbase/lib/commons-digester-1.8.jar:/home/hadoop/hbase/lib/commons-el-1.0.jar:/home/hadoop/hbase/lib/commons-fileupload-1.3.1.jar:/home/hadoop/hbase/lib/commons-httpclient-3.1.jar:/home/hadoop/hbase/lib/commons-io-2.4.jar:/home/hadoop/hbase/lib/commons-lang-2.6.jar:/home/hadoop/hbase/lib/commons-logging-1.2.jar:/home/hadoop/hbase/lib/commons-math-2.2.jar:/home/hadoop/hbase/lib/commons-math3-3.1.1.jar:/home/hadoop/hbase/lib/commons-net-3.1.jar:/home/hadoop/hbase/lib/disruptor-3.3.0.jar:/home/hadoop/hbase/lib/esapi-2.1.0.1.jar:/home/hadoop/hbase/lib/findbugs-annotations-1.3.9-1.jar:/home/hadoop/hbase/lib/guava-12.0.1.jar:/home/hadoop/hbase/lib/guice-3.0.jar:/home/hadoop/hbase/lib/guice-servlet-3.0.jar:/home/hadoop/hbase/lib/hadoop-annotations-2.5.1.jar:/home/hadoop/hbase/lib/hadoop-auth-2.5.1.jar:/home/hadoop/hbase/lib/hadoop-client-2.5.1.jar:/home/hadoop/hbase/lib/hadoop-common-2.5.1.jar:/home/hadoop/hbase/lib/hadoop-hdfs-2.5.1.jar:/home/hadoop/hbase/lib/hadoop-mapreduce-client-app-2.5.1.jar:/home/hadoop/hbase/lib/hadoop-mapreduce-client-common-2.5.1.jar:/home/hadoop/hbase/lib/hadoop-mapreduce-client-core-2.5.1.jar:/home/hadoop/hbase/lib/hadoop-mapreduce-client-jobclient-2.5.1.jar:/home/hadoop/hbase/lib/hadoop-mapreduce-client-shuffle-2.5.1.jar:/home/hadoop/hbase/lib/hadoop-yarn-api-2.5.1.jar:/home/hadoop/hbase/lib/hadoop-yarn-client-2.5.1.jar:/home/hadoop/hbase/lib/hadoop-yarn-common-2.5.1.jar:/home/hadoop/hbase/lib/hadoop-yarn-server-common-2.5.1.jar:/home/hadoop/hbase/lib/hbase-annotations-1.2.2.jar:/home/hadoop/hbase/lib/hbase-annotations-1.2.2-tests.jar:/home/hadoop/hbase/lib/hbase-client-1.2.2.jar:/home/hadoop/hbase/lib/hbase-common-1.2.2.jar:/home/hadoop/hbase/lib/hbase-common-1.2.2-tests.jar:/home/hadoop/hbase/lib/hbase-examples-1.2.2.jar:/home/hadoop/hbase/lib/hbase-external-blockcache-1.2.2.jar:/home/hadoop/hbase/lib/hbase-hadoop2-compat-1.2.2.jar:/home/hadoop/hbase/lib/hbase-hadoop-compat-1.2.2.jar:/home/hadoop/hbase/lib/hbase-it-1.2.2.jar:/home/hadoop/hbase/lib/hbase-it-1.2.2-tests.jar:/home/hadoop/hbase/lib/hbase-prefix-tree-1.2.2.jar:/home/hadoop/hbase/lib/hbase-procedure-1.2.2.jar:/home/hadoop/hbase/lib/hbase-protocol-1.2.2.jar:/home/hadoop/hbase/lib/hbase-resource-bundle-1.2.2.jar:/home/hadoop/hbase/lib/hbase-rest-1.2.2.jar:/home/hadoop/hbase/lib/hbase-server-1.2.2.jar:/home/hadoop/hbase/lib/hbase-server-1.2.2-tests.jar:/home/hadoop/hbase/lib/hbase-shell-1.2.2.jar:/home/hadoop/hbase/lib/hbase-thrift-1.2.2.jar:/home/hadoop/hbase/lib/htrace-core-3.1.0-incubating.jar:/home/hadoop/hbase/lib/httpclient-4.2.5.jar:/home/hadoop/hbase/lib/httpcore-4.4.1.jar:/home/hadoop/hbase/lib/jackson-core-asl-1.9.13.jar:/home/hadoop/hbase/lib/jackson-jaxrs-1.9.13.jar:/home/hadoop/hbase/lib/jackson-mapper-asl-1.9.13.jar:/home/hadoop/hbase/lib/jackson-xc-1.9.13.jar:/home/hadoop/hbase/lib/jamon-runtime-2.4.1.jar:/home/hadoop/hbase/lib/jasper-compiler-5.5.23.jar:/home/hadoop/hbase/lib/jasper-runtime-5.5.23.jar:/home/hadoop/hbase/lib/javax.inject-1.jar:/home/hadoop/hbase/lib/java-xmlbuilder-0.4.jar:/home/hadoop/hbase/lib/jaxb-api-2.2.2.jar:/home/hadoop/hbase/lib/jaxb-impl-2.2.3-1.jar:/home/hadoop/hbase/lib/jcodings-1.0.8.jar:/home/hadoop/hbase/lib/jersey-client-1.9.jar:/home/hadoop/hbase/lib/jersey-core-1.9.jar:/home/hadoop/hbase/lib/jersey-guice-1.9.jar:/home/hadoop/hbase/lib/jersey-json-1.9.jar:/home/hadoop/hbase/lib/jersey-server-1.9.jar:/home/hadoop/hbase/lib/jets3t-0.9.0.jar:/home/hadoop/hbase/lib/jettison-1.3.3.jar:/home/hadoop/hbase/lib/jetty-6.1.26.jar:/home/hadoop/hbase/lib/jetty-sslengine-6.1.26.jar:/home/hadoop/hbase/lib/jetty-util-6.1.26.jar:/home/hadoop/hbase/lib/joni-2.1.2.jar:/home/hadoop/hbase/lib/jruby-complete-1.6.8.jar:/home/hadoop/hbase/lib/jsch-0.1.42.jar:/home/hadoop/hbase/lib/jsp-2.1-6.1.14.jar:/home/hadoop/hbase/lib/jsp-api-2.1-6.1.14.jar:/home/hadoop/hbase/lib/jsr305-1.3.9.jar:/home/hadoop/hbase/lib/junit-4.12.jar:/home/hadoop/hbase/lib/leveldbjni-all-1.8.jar:/home/hadoop/hbase/lib/libthrift-0.9.3.jar:/home/hadoop/hbase/lib/log4j-1.2.17.jar:/home/hadoop/hbase/lib/metrics-core-2.2.0.jar:/home/hadoop/hbase/lib/nekohtml-1.9.16.jar:/home/hadoop/hbase/lib/netty-all-4.0.23.Final.jar:/home/hadoop/hbase/lib/paranamer-2.3.jar:/home/hadoop/hbase/lib/protobuf-java-2.5.0.jar:/home/hadoop/hbase/lib/servlet-api-2.5-6.1.14.jar:/home/hadoop/hbase/lib/servlet-api-2.5.jar:/home/hadoop/hbase/lib/slf4j-api-1.7.7.jar:/home/hadoop/hbase/lib/slf4j-log4j12-1.7.5.jar:/home/hadoop/hbase/lib/snappy-java-1.0.4.1.jar:/home/hadoop/hbase/lib/spymemcached-2.11.6.jar:/home/hadoop/hbase/lib/xalan-2.7.0.jar:/home/hadoop/hbase/lib/xml-apis-1.3.03.jar:/home/hadoop/hbase/lib/xml-apis-ext-1.3.04.jar:/home/hadoop/hbase/lib/xmlenc-0.52.jar:/home/hadoop/hbase/lib/xom-1.2.5.jar:/home/hadoop/hbase/lib/xz-1.0.jar:/home/hadoop/hbase/lib/zookeeper-3.4.6.jar:/home/hadoop/hadoop-2.7.2/etc/hadoop:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/apacheds-i18n-2.0.0-M15.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jersey-core-1.9.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/asm-3.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/netty-3.6.2.Final.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jetty-util-6.1.26.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-httpclient-3.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/curator-client-2.7.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jackson-xc-1.9.13.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-digester-1.8.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/httpcore-4.2.5.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-net-3.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/xmlenc-0.52.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jaxb-api-2.2.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jsch-0.1.42.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-logging-1.1.3.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/stax-api-1.0-2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jersey-server-1.9.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/curator-recipes-2.7.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/htrace-core-3.1.0-incubating.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/activation-1.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-beanutils-1.7.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/log4j-1.2.17.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-codec-1.4.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-compress-1.4.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-cli-1.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/mockito-all-1.8.5.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/api-asn1-api-1.0.0-M20.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jetty-6.1.26.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/slf4j-api-1.7.10.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jackson-jaxrs-1.9.13.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jsr305-3.0.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-configuration-1.6.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-beanutils-core-1.8.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jackson-core-asl-1.9.13.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/guava-11.0.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-collections-3.2.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jaxb-impl-2.2.3-1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/api-util-1.0.0-M20.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/curator-framework-2.7.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-math3-3.1.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jsp-api-2.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jettison-1.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jersey-json-1.9.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/xz-1.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/junit-4.11.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jackson-mapper-asl-1.9.13.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-lang-2.6.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/hadoop-annotations-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/java-xmlbuilder-0.4.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/protobuf-java-2.5.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/apacheds-kerberos-codec-2.0.0-M15.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/avro-1.7.4.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/commons-io-2.4.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/snappy-java-1.0.4.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/hadoop-auth-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/hamcrest-core-1.3.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/jets3t-0.9.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/gson-2.2.4.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/httpclient-4.2.5.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/paranamer-2.3.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/servlet-api-2.5.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/lib/zookeeper-3.4.6.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/hadoop-common-2.7.2-tests.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/hadoop-common-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/common/hadoop-nfs-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/jersey-core-1.9.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/commons-daemon-1.0.13.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/asm-3.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/netty-3.6.2.Final.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/jetty-util-6.1.26.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/xml-apis-1.3.04.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/xmlenc-0.52.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/commons-logging-1.1.3.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/leveldbjni-all-1.8.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/jersey-server-1.9.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/htrace-core-3.1.0-incubating.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/log4j-1.2.17.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/commons-codec-1.4.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/commons-cli-1.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/jetty-6.1.26.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/jsr305-3.0.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/jackson-core-asl-1.9.13.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/guava-11.0.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/xercesImpl-2.9.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/jackson-mapper-asl-1.9.13.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/commons-lang-2.6.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/protobuf-java-2.5.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/commons-io-2.4.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/netty-all-4.0.23.Final.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/servlet-api-2.5.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/hadoop-hdfs-nfs-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/hadoop-hdfs-2.7.2-tests.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/hdfs/hadoop-hdfs-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jersey-core-1.9.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/guice-3.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/asm-3.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/netty-3.6.2.Final.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/aopalliance-1.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jetty-util-6.1.26.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jackson-xc-1.9.13.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jaxb-api-2.2.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/commons-logging-1.1.3.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/stax-api-1.0-2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/leveldbjni-all-1.8.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jersey-server-1.9.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/activation-1.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/log4j-1.2.17.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/commons-codec-1.4.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/javax.inject-1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/zookeeper-3.4.6-tests.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/commons-compress-1.4.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/commons-cli-1.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jetty-6.1.26.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jackson-jaxrs-1.9.13.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jsr305-3.0.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jersey-guice-1.9.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jackson-core-asl-1.9.13.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/guava-11.0.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/commons-collections-3.2.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jaxb-impl-2.2.3-1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jersey-client-1.9.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jettison-1.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jersey-json-1.9.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/xz-1.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/jackson-mapper-asl-1.9.13.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/commons-lang-2.6.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/protobuf-java-2.5.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/guice-servlet-3.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/commons-io-2.4.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/servlet-api-2.5.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/zookeeper-3.4.6.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/hadoop-yarn-server-resourcemanager-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/hadoop-yarn-server-common-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/hadoop-yarn-server-web-proxy-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/hadoop-yarn-api-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/hadoop-yarn-client-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/hadoop-yarn-server-tests-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/hadoop-yarn-registry-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/hadoop-yarn-server-applicationhistoryservice-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/hadoop-yarn-server-nodemanager-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/hadoop-yarn-common-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/yarn/hadoop-yarn-server-sharedcachemanager-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/jersey-core-1.9.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/guice-3.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/asm-3.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/netty-3.6.2.Final.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/aopalliance-1.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/leveldbjni-all-1.8.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/jersey-server-1.9.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/log4j-1.2.17.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/javax.inject-1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/commons-compress-1.4.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/jersey-guice-1.9.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/jackson-core-asl-1.9.13.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/xz-1.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/junit-4.11.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/jackson-mapper-asl-1.9.13.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/hadoop-annotations-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/protobuf-java-2.5.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/guice-servlet-3.0.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/avro-1.7.4.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/commons-io-2.4.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/snappy-java-1.0.4.1.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/hamcrest-core-1.3.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/paranamer-2.3.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-client-app-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-client-shuffle-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-plugins-2.7.2.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.2-tests.jar:/home/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.7.2.jar:/home/hadoop/hadoop-2.7.2/contrib/capacity-scheduler/*.jar
2020-05-23 16:41:13,983 INFO  [main] zookeeper.ZooKeeper: Client environment:java.library.path=/home/hadoop/hadoop-2.7.2/lib/native
2020-05-23 16:41:13,984 INFO  [main] zookeeper.ZooKeeper: Client environment:java.io.tmpdir=/tmp
2020-05-23 16:41:13,985 INFO  [main] zookeeper.ZooKeeper: Client environment:java.compiler=<NA>
2020-05-23 16:41:13,985 INFO  [main] zookeeper.ZooKeeper: Client environment:os.name=Linux
2020-05-23 16:41:13,985 INFO  [main] zookeeper.ZooKeeper: Client environment:os.arch=amd64
2020-05-23 16:41:13,985 INFO  [main] zookeeper.ZooKeeper: Client environment:os.version=2.6.32-431.el6.x86_64
2020-05-23 16:41:13,986 INFO  [main] zookeeper.ZooKeeper: Client environment:user.name=hadoop
2020-05-23 16:41:13,986 INFO  [main] zookeeper.ZooKeeper: Client environment:user.home=/home/hadoop
2020-05-23 16:41:13,986 INFO  [main] zookeeper.ZooKeeper: Client environment:user.dir=/home/hadoop
2020-05-23 16:41:13,989 INFO  [main] zookeeper.ZooKeeper: Initiating client connection, connectString=master1.hadoop:2181,slave2.hadoop:2181,slave3.hadoop:2181 sessionTimeout=90000 watcher=hconnection-0x56ace4000x0, quorum=master1.hadoop:2181,slave2.hadoop:2181,slave3.hadoop:2181, baseZNode=/hbase
2020-05-23 16:41:14,403 INFO  [main-SendThread(master1.hadoop:2181)] zookeeper.ClientCnxn: Opening socket connection to server master1.hadoop/192.168.112.210:2181. Will not attempt to authenticate using SASL (unknown error)
2020-05-23 16:41:14,427 INFO  [main-SendThread(master1.hadoop:2181)] zookeeper.ClientCnxn: Socket connection established to master1.hadoop/192.168.112.210:2181, initiating session
2020-05-23 16:41:14,477 INFO  [main-SendThread(master1.hadoop:2181)] zookeeper.ClientCnxn: Session establishment complete on server master1.hadoop/192.168.112.210:2181, sessionid = 0x724072e9280009, negotiated timeout = 90000
2020-05-23 16:41:14,964 INFO  [main] util.RegionSizeCalculator: Calculating region sizes for table "test:test".
2020-05-23 16:41:17,211 INFO  [main] client.ConnectionManager$HConnectionImplementation: Closing master protocol: MasterService
2020-05-23 16:41:17,212 INFO  [main] client.ConnectionManager$HConnectionImplementation: Closing zookeeper sessionid=0x724072e9280009
2020-05-23 16:41:17,218 INFO  [main-EventThread] zookeeper.ClientCnxn: EventThread shut down
2020-05-23 16:41:17,219 INFO  [main] zookeeper.ZooKeeper: Session: 0x724072e9280009 closed
2020-05-23 16:41:17,501 INFO  [main] mapreduce.JobSubmitter: number of splits:1
2020-05-23 16:41:17,538 INFO  [main] Configuration.deprecation: io.bytes.per.checksum is deprecated. Instead, use dfs.bytes-per-checksum
2020-05-23 16:41:18,619 INFO  [main] mapreduce.JobSubmitter: Submitting tokens for job: job_1590219117109_0003
2020-05-23 16:41:20,433 INFO  [main] impl.YarnClientImpl: Submitted application application_1590219117109_0003
2020-05-23 16:41:20,589 INFO  [main] mapreduce.Job: The url to track the job: http://master1.hadoop:8088/proxy/application_1590219117109_0003/
2020-05-23 16:41:20,590 INFO  [main] mapreduce.Job: Running job: job_1590219117109_0003
2020-05-23 16:41:48,592 INFO  [main] mapreduce.Job: Job job_1590219117109_0003 running in uber mode : false
2020-05-23 16:41:48,594 INFO  [main] mapreduce.Job:  map 0% reduce 0%
2020-05-23 16:42:02,228 INFO  [main] mapreduce.Job:  map 100% reduce 0%
2020-05-23 16:42:03,257 INFO  [main] mapreduce.Job: Job job_1590219117109_0003 completed successfully
2020-05-23 16:42:03,615 INFO  [main] mapreduce.Job: Counters: 31
	File System Counters
		FILE: Number of bytes read=0
		FILE: Number of bytes written=147864
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=74
		HDFS: Number of bytes written=0
		HDFS: Number of read operations=1
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=0
	Job Counters 
		Launched map tasks=1
		Data-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=11764
		Total time spent by all reduces in occupied slots (ms)=0
		Total time spent by all map tasks (ms)=11764
		Total vcore-seconds taken by all map tasks=11764
		Total megabyte-seconds taken by all map tasks=12046336
	Map-Reduce Framework
		Map input records=3
		Map output records=0
		Input split bytes=74
		Spilled Records=0
		Failed Shuffles=0
		Merged Map outputs=0
		GC time elapsed (ms)=150
		CPU time spent (ms)=2840
		Physical memory (bytes) snapshot=118710272
		Virtual memory (bytes) snapshot=2067132416
		Total committed heap usage (bytes)=30474240
	org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper$Counters
		ROWS=3
	File Input Format Counters 
		Bytes Read=0
	File Output Format Counters 
		Bytes Written=0
```

## 3.Hive over Hbase
通过新建hive外部表，关联hbase表，使用sql即可统计出来hbase表的行数。

hive over hbase 表的建表语句为：
```
/*创建hive与hbase的关联表*/
CREATE TABLE hive_hbase_1(key INT,value STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key,cf:val")
TBLPROPERTIES("hbase.table.name"="t_hive","hbase.table.default.storage.type"="binary");
```
```
/*hive关联已经存在的hbase*/
CREATE EXTERNAL TABLE hive_hbase_1(key INT,value STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key,cf:val")
TBLPROPERTIES("hbase.table.name"="t_hive","hbase.table.default.storage.type"="binary");
```

Hive关联Hbase表具体可以看前面这篇文章，[Hive整合Hbase](https://mp.weixin.qq.com/s/glRyDRUwnDh1JV35JEdYJA)。

**猜你喜欢**

[SparkStreaming实时计算pv和uv](https://mp.weixin.qq.com/s/e0hdRpWPIq2hxzQpkc-GqQ)

[Flink状态管理与状态一致性（长文）](https://mp.weixin.qq.com/s/hZeO7LtUwzZl0yK8eC8nmQ)

[Flink实时计算topN热榜](https://mp.weixin.qq.com/s/9K3oclvWDt0y14DIkDmQrw)

[数仓建模分层理论](https://mp.weixin.qq.com/s/8rpDyo41Kr4r_2wp5hirVA)

[数仓建模方法论](https://mp.weixin.qq.com/s/CTyynCUCLB2lq9S1ujRNaQ)

[大数据组件重点学习这几个](https://mp.weixin.qq.com/s/4redHF0e7vCWFqv8t20Rjg)
