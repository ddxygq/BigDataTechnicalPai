## Flink on Yarn安装

步骤很简单，既然要基于`yarn`，前提是`hadoop`已经安装好了，我们选择`hadoop`集群的一个节点，安装一个`flink`客户端即可。我们平时提交任务，包括mr、spark任务等，也会去安装一个`spark`客户端。



选择一个节点，这个节点通常会用它提交任务。

下载flink

wget http://archive.apache.org/dist/flink/flink-1.13.5/flink-1.13.5-bin-scala_2.11.tgz



解压

tar -zxvf flink-1.13.5-bin-scala_2.11.tgz



配置环境

export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_CLASSPATH=`hadoop classpath`



启动一个flink-session

bin/yarn-session.sh -d -s 2 -jm 1024 -tm 2048 -qu [yarn队列名] -nm wordcount

比如这里生成了一个`application_1633924491541_2461`



进入yarn界面

http://cdh-003:8088/cluster/scheduler

点击