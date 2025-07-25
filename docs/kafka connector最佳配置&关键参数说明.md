### 最佳使用配置

```

topic.prefix=pre_topic
// initial是全量表结构+全量数据+增量数据，如果仅需要全量表结构+增量数据，选择schema_only
snapshot.mode=initial
max.batch.size=2048
max.queue.size=10240
lob.enabled=true
decimal.handling.mode=string
// 全量数据并发查询的线程数，如果资源足够可以增大到16或者更大
snapshot.max.threads=8
schema.history.internal.kafka.topic=schema_history_internal
schema.history.internal.kafka.bootstrap.servers=<IP>:<PORT>
// 避免由于其他非表的DDL解析失败，导致任务失败
schema.history.internal.skip.unparseable.ddl=true
// 仅快照存储捕获的表的表结构
schema.history.internal.store.only.captured.tables.ddl=true

```

### 关键参数说明:

其他所有参数可以参考：  [https://debezium.io/documentation/reference/2.4/connectors/oracle.html#required-debezium-oracle-connector-configuration-properties](https://debezium.io/documentation/reference/2.4/connectors/oracle.html#required-debezium-oracle-connector-configuration-properties)  

|参数名|默认值|参数说明|使用场景|
|---|---|---|---|
|topic.prefix|没有默认值|用于为连接器从中捕获更改的数据库服务器提供的前缀命名空间，举例说明,topic.prefix=pre,任务启动后会生成topic,- pre.schema_name：此topic存储数据库的schema的数据信息
- pre.schema_name.table_name: 此topic存储数据库表（schema_name.table_name）的数据信息，只有当表中有数据事件才会生成
|用于生成的topic的命名的前缀|
|topic.delimiter|.|topic的分隔符|如生成topic:,topic前缀.schema名称.表名称|
|snapshot.mode|initial|连接器对捕获的表进行快照的模式，取值范围如下：,- always：连接器  **每次启动**  时都会进行快照（表结构和表的全量数据）
- initial：连接器  **首次启动**  时执行快照（表结构和表的全量数据），快照完成后连接器开始捕获（增量数据）并记录目标表发生的表结构和数据更改，后续启动时不会再次执行快照
- initial_only：（仅表结构和全量数据），连接器首次启动时执行快照（表结构和数据），在目标表发生连接器启动后的首次更改时中止快照，且连接器不处理目标表发生的任何后续更改
- schema_only：仅含表结构+增量数据
- schema_only_recovery：基于schema_only模式的恢复模式，可用于连接器意外断连后再次重启时，连接器启动后会执行快照恢复损坏或丢失的历史主题，照完成后，连接器的表现同schema_only模式。  **仅在连接器上一次意外断连时间点至快照时间点期间未发生表结构更改的情况下，才能安全使用此模式**  。  

|全量快照的模式,如果需要全量（表结构和全量数据）+增量数据，就选用initial,如果需要仅全量（表结构和全量数据），就选用initial_only,如果需要仅表结构+增量数据，就选用schema_only,如果选用schema_only模式，但是连接器意外断连，需要恢复重启，就选用schema_only_recovery更改config，然后重启，连接器启动后会执行快照恢复损坏或丢失的历史主题。  **仅在连接器上一次意外断连时间点至快照时间点期间未发生表结构更改的情况下，才能安全使用此模式**  。|
|max.batch.size|2048|连接器每次迭代期间要处理的单批事件的最大大小，正整数值|就是中间缓冲队列（debezium到kafka connect发送的中间队列）的处理的每一批次的事件数量，如果机器内存满足情况下，可调大。,建议max.batch.size=2048|
|max.queue.size|9182|阻塞队列可以容纳的最大记录数，正整数值。|中间缓冲队列（debezium到kafka connect发送的中间队列）的阻塞队列的大小，如果机器内存满足情况下，可调大。,建议max.queue.size=10240|
|lob.enabled|false|控制是否开启lob数据同步|如果表中有lob数据类型，需要设置该值为true|
|decimal.handling.mode|precise|连接器处理NUMBER、DECIMAL和NUMERIC列的浮点值的模式，有三种取值,precise,double,string|建议使用decimal.handling.mode=string即可|
|snapshot.max.threads|1|全量读取快照的线程数量|建议调大以加快全量快照，建议设置为8。,|
|schema.history.internal.kafka.topic |无默认值|连接器存储数据库模式历史记录的 Kafka 主题的全名。|连接器用来存储表结构的kafka topic|
|schema.history.internal.kafka.bootstrap.servers|无默认值|连接器用于建立与 Kafka 集群的初始连接的主机/端口对列表。此连接用于检索连接器先前存储的数据库架构历史记录，以及写入从源数据库读取的每个 DDL 语句。每个主机/端口对都应指向 Kafka Connect 进程使用的同一 Kafka 集群。|需要添加kafka的server的IP和端口|
|schema.history.internal.skip.unparseable.ddl|false|增量DDL解析失败，是否跳过忽略|如果遇到增量DDL解析失败的情况，可以修改配置文件，将此选项设置为true，并重启任务|
|schema.history.internal.store.only.captured.tables.ddl|false|一个布尔值，用于指定连接器是记录架构或数据库中所有表的架构结构，还是仅记录指定要捕获的表的架构结构。  
请指定以下值之一：,  `false`  （默认）,在数据库快照期间，连接器会记录数据库中所有非系统表的架构数据，包括未指定捕获的表。最好保留默认设置。如果您之后决定捕获最初未指定捕获的表中的更改，连接器可以轻松开始捕获这些表中的数据，因为它们的架构结构已存储在架构历史记录主题中。Debezium 需要表的架构历史记录，以便识别发生更改事件时存在的结构。,true:,在数据库快照期间，连接器仅记录 Debezium 捕获变更事件的表的表架构。如果您更改默认值，并且稍后将连接器配置为从数据库中的其他表捕获数据，则连接器将缺少从这些表捕获变更事件所需的架构信息。|默认情况下，连接器会快照所有非系统表的元数据。,如果仅需快照捕获的表（schema.include.list和table.include.list）的元数据，建议设置为true。|
|schema.history.internal.producer.<kafka的生产者参数>|无默认值|debezium提供给用户可以自行配置schema.history.internal.kafka.topic 的生产者的配置信息|如果设置该topic的retries重试次数为3,可以设置,schema.history.internal.producer.retries=3|
|schema.history.internal.consumer.<kafka的消费者参数>|无默认值|debezium提供给用户可以自行配置schema.history.internal.kafka.topic 的消费者的配置信息|如果设置该topic的消费者的session.timeout.ms会话超时时间为20000ms,可以设置,schema.history.internal.consumer.session.timeout.ms=30000|


