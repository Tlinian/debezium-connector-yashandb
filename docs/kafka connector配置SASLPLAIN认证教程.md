## kafka connector配置SASL/PLAIN认证教程

### kafka 服务端配置

##### （1）修改server.properties

```
# 启用SASL_PLAINTEXT监听器
listeners=SASL_PLAINTEXT://0.0.0.0:9092
advertised.listeners=SASL_PLAINTEXT://<主机名或IP>:9092
# 安全协议与SASL机制
security.inter.broker.protocol=SASL_PLAINTEXT
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN

# 可选：配置JAAS直接嵌入（也可以通过步骤二）
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" \
  password="admin-secret" \
  user_admin="admin-secret" \
  user_alice="alice-secret";
```

##### （2）创建JAAS文件

在`config/kafka_server_jaas.conf`中定义认证信息：

```
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="admin-secret"
  user_admin="admin-secret"
  user_alice="alice-secret";
};
```

通过环境变量加载JAAS：（一定要填绝对路径）

```
export KAFKA_OPTS="-Djava.security.auth.login.config=/path/to/kafka_server_jaas.conf"
```

##### （3）启动kafka服务

```
bin/kafka-server-start.sh config/server.properties
```

### Kafka Connect配置

##### （1）修改connect-distributed.properties文件，补充以下几行

```
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='admin-secret';
# 账号和密码一定要跟sasl文件中的对应上
consumer.security.protocol=SASL_PLAINTEXT
consumer.sasl.mechanism=PLAIN
consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='admin-secret';

producer.security.protocol=SASL_PLAINTEXT
producer.sasl.mechanism=PLAIN
producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='admin-secret';
```

##### （2）在启动kafka connect前设置一下环境变量加载JAAS（一定要填绝对路径）

```
export KAFKA_OPTS="-Djava.security.auth.login.config=/path/to/kafka_server_jaas.conf"
```

##### （3）启动kafka connect

```
bin/connect-distributed.sh config/connect-distributed.properties
```

### YashanDB Kafka Connector配置

##### （1）准备post请求所需的请求的json文件（yashandb_task.json）

注意配置sasl参数：

- security.protocol
- sasl.mechanism
- sasl.jaas.config
- schema.history.internal.consumer.security.protocol
- schema.history.internal.consumer.sasl.mechanism
- schema.history.internal.consumer.sasl.jaas.config
- schema.history.internal.producer.security.protocol
- schema.history.internal.producer.sasl.mechanism
- schema.history.internal.producer.sasl.jaas.config

```
{
   "name": "yashandb_connector718",
   "config": {
       "connector.class": "io.debezium.connector.yashandb.YashanDBConnector",
       "database.url": "jdbc:yasdb://172.16.90.71:2688/SYS",
       "database.hostname": "172.16.90.71",
       "database.port": "2688",
       "database.user": "SYS",
       "database.password": "Cod-2022",
       "database.dbname": "TEST",
       "schema.include.list": "TESTFF1",
       "store.only.captured.tables.ddl": "true",
       "schema.history.internal.store.only.captured.tables.ddl": "true",
       "schema.history.internal.kafka.bootstrap.servers": "172.16.90.71:9093",
       "database.ystream.server.name": "testdebezium",
       "schema.history.internal.kafka.topic": "serverName23-yashandb-dbhistory718",
       "lob.enabled": "true",
       "security.protocol": "SASL_PLAINTEXT",
       "sasl.mechanism": "PLAIN",
       "sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='admin-secret';",
       "schema.history.internal.consumer.security.protocol": "SASL_PLAINTEXT",
       "schema.history.internal.consumer.sasl.mechanism": "PLAIN",
       "schema.history.internal.consumer.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='admin-secret';",
       "schema.history.internal.producer.security.protocol": "SASL_PLAINTEXT",
       "schema.history.internal.producer.sasl.mechanism": "PLAIN",
       "schema.history.internal.producer.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='admin-secret';",
       "topic.prefix": "m-topoc"
   }
}
```

##### （2）发送post创建connector任务

**注意：在window发送可能存在编码问题导致sasl连不上，建议跟kafka同一个环境发送请求**

```
curl -X POST -H "Content-Type: application/json" -d @yashandb_task.json http://172.16.90.71:8083/connectors
```

##### （3）查看kafka connect日志，可以看出已经成功创建任务

```
[2025-07-18 10:45:58,744] INFO [yashandb_connector718|task-0] Creating thread debezium-yashandbconnector-m-topoc-SignalProcessor (io.debezium.util.Threads:288)
[2025-07-18 10:45:58,744] INFO [yashandb_connector718|task-0] WorkerSourceTask{id=yashandb_connector718-0} Source task finished initialization and start (org.apache.kafka.connect.runtime.AbstractWorkerSourceTask:280)
[2025-07-18 10:45:58,748] INFO [yashandb_connector718|task-0] No previous offset has been found. (io.debezium.connector.yashandb.YashanDBSnapshotChangeEventSource:83)
[2025-07-18 10:45:58,748] INFO [yashandb_connector718|task-0] According to the connector configuration both schema and data will be snapshot. (io.debezium.connector.yashandb.YashanDBSnapshotChangeEventSource:88)
[2025-07-18 10:45:58,750] INFO [yashandb_connector718|task-0] Snapshot step 1 - Preparing (io.debezium.relational.RelationalSnapshotChangeEventSource:121)
[2025-07-18 10:45:58,751] INFO [yashandb_connector718|task-0] Snapshot step 2 - Determining captured tables (io.debezium.relational.RelationalSnapshotChangeEventSource:130)
[2025-07-18 10:45:58,786] INFO [yashandb_connector718|task-0] Adding table TESTFF1.LONGRAWTAB to the list of capture schema tables (io.debezium.relational.RelationalSnapshotChangeEventSource:289)
[2025-07-18 10:45:58,786] INFO [yashandb_connector718|task-0] Adding table TESTFF1.LTEST1 to the list of capture schema tables (io.debezium.relational.RelationalSnapshotChangeEventSource:289)
[2025-07-18 10:45:58,786] INFO [yashandb_connector718|task-0] Adding table TESTFF1.CLOBMAPLONG to the list of capture schema tables (io.debezium.relational.RelationalSnapshotChangeEventSource:289)

```

