![](https://oss.ikeguang.com/image/2022-07-29/logstash.png)
### 1. mysql建表test;
### 2. 安装logstash（跟es版本一致）
```
# 下载
wget https://repo.huaweicloud.com/logstash/7.14.2/logstash-7.14.2-linux-x86_64.tar.gz
# 解压
tar -zxvf logstash-7.14.2-linux-x86_64.tar.gz
# 需要mysql-connector-java-5.1.40.jar，随便放到比如目录
# /var/lib/hadoop-hdfs/logstash-7.14.2/lib/mysql-connector-java-5.1.40.jar
```
### 3. 新建`es`索引`test`
```
curl -u elastic:changeme -X PUT  http://192.168.20.130:9200/test -H 'Content-Type: application/json' -d'
{
  "settings" : {
      "number_of_shards" : 1,
      "number_of_replicas" : 1
   },
    "mappings" : {
        "properties": {
          "id": {
            "type" : "long"
          }, 
          "type": {
            "type": "keyword"
          }, 
          "keyword_1": {
            "type": "text",
            "analyzer" : "ik_smart"
          }, 
          "keyword_2": {
            "type": "text",
            "analyzer" : "ik_smart"
          },
          "keyword_3": {
            "type": "text",
            "analyzer" : "ik_smart"
          },
          "data": {
            "type": "keyword"
          },
          "created_at": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
          }, 
          "updated_at": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
          }
      }
    }
}'
```
### 4. 编辑配置文件，`vim ~/script/logstash/logstash_mysql2es.conf`
```
input {
  stdin{
  }
  jdbc{
    # 连接的数据库地址和数据库，指定编码格式，禁用ssl协议，设定自动重连
    # 此处10.112.103.2为MySQL所在IP地址，也是elastic search所在IP地址
    jdbc_connection_string => "jdbc:mysql://192.168.13.28:3306/test?characterEncoding=UTF-8&useSSL=FALSE&autoReconnect=true"
    #数据库用户名
    jdbc_user => "root"
    # 数据库用户名对应的密码
    jdbc_password => "root"
    # jar包存放位置
    jdbc_driver_library => "/var/lib/hadoop-hdfs/logstash-7.14.2/lib/mysql-connector-java-5.1.40.jar"
    jdbc_driver_class => "com.mysql.jdbc.Driver"
    jdbc_default_timezone => "Asia/Shanghai"
    jdbc_paging_enabled => "true"
    jdbc_page_size => "320000"
    lowercase_column_names => false
    statement => "select id, type, tags, title from test"
  }
}
filter {
    # 移除无关的字段
    mutate {
        remove_field => ["@version", "@timestamp"]
    }
}
output {
  elasticsearch {
    hosts => ["http://192.168.20.130:9200"]
    user => "elastic"
    password => "changeme"
    index => "test"
    document_type => "_doc"
	# 将字段type和id作为文档id
    document_id => "%{type}_%{id}"
  }
  stdout {
    codec => json_lines
  }
}
```
**重要配置参数说明**：
1. `remove_field => ["@version", "@timestamp"]`:  默认`logstash`会添加这两个字段，这里去掉；
1. `document_id => "%{type}_%{id}"`: 将两个字段拼接作为es的文档id；

### 5. 启动任务
```
./logstash-7.14.2/bin/logstash -f script/logstash/logstash_mysql2es.conf
```

**参考资料**
1. [Jdbc input plugin](https://www.elastic.co/guide/en/logstash/current/plugins-inputs-jdbc.html)
1. [Elasticsearch output plugin](https://www.elastic.co/guide/en/logstash/current/output-plugins.html)
1. [通过logstash将mysql数据同步到elastic search](https://www.jianshu.com/p/9ffdf97e7a22)