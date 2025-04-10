## Debezium Connector YashanDB

## 1. 概述

Debezium Connector YashanDB连接器同步全量快照数据，捕获并记录YashanDB数据库中发生的行级更改，包含连接器运行时添加的表。您可以配置YashanDB连接器，使其为特定的schema和表捕获更改事件，将更改事件同步到Kafka。

## 2. 版本兼容

| Connector Version | YashanDB Version                   | YashanDB Jdbc Version           | Debezium Version | Kafka Version | Java Version |
| ----------------- | ---------------------------------- | ------------------------------- | ---------------- | ------------- | ------------ |
| 2.4.2.x           | 支持YashanDB YStream的YashanDB版本 | 支持YashanDB YStream的Jdbc 版本 | 2.4.2.Final      | 2.x,3.x       | 11+          |

## 3. 部署

### 3.1 前置准备

- Apache Zookeeper、Apache Kafka、 kafka Connect 已安装
  - 参考：[Apache Kafka](https://kafka.apache.org/quickstart)
- 准备debezium-connector-yashandb-2.4.2.jar
- 准备YashanDB JDBC
- 准备YashanDB YStream

### 3.2 配置YashanDB

#### 3.2.1 YStream实时获取源端数据

增量数据依赖YStream实时获取YashanDB的已提交数据。

当您使用YashanDB作为源使用含增量同步的任务时，您需要首先在YashanDB上为YStream分配内存池，如下：

```sql
ALTER SYSTEM SET STREAM_POOL_SIZE = streamPoolSize;
```

> **Caution**：
>
> 未配置该参数，将导致任务运行失败，该参数为全局参数，请参考YashanDB官网文档获取更进一步帮助。

#### 3.2.2 开启附加日志

读取增量数据变更需要开启附加日志，当您需要监听库下全部对象时（包含新增对象），可开全库附加日志，方式如下：

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG TABLE TYPE (HEAP);
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA ( ALL) COLUMNS;

```

当您仅需要监听某些表时，可开启表级附加日志，方式如下：

```sql
ALTER TABLE tablename ADD SUPPLEMENTAL LOG DATA ( ALL ) COLUMNS;

```

> **Caution**：
>
> 需要额外注意的是，忽略开启附加日志或开启附加日志不正确会导致数据丢失甚至任务失败。

#### 3.2.3 配置YStream服务

详细配置教程参考：[DBMS_YSTREAM_ADM | YashanDB Doc (yasdb.com)](https://cod-doc.yasdb.com/yashandb/23.3/zh/开发手册/PL参考手册/内置高级包/DBMS_YSTREAM_ADM.html)

##### 3.2.3.1 创建YStream 服务

```sql
DBMS_YSTREAM_ADM.CREATE(
server_name  IN VARCHAR(64),
connect_user  IN VARCHAR(64) DEFAULT NULL,
start_scn   IN BIGINT DEFAULT NULL);
```



start_scn 通过查询select CURRENT_SCN from V$DATABASE获取。

```
EXEC DBMS_YSTREAM_ADM.CREATE('serverName', 'connect_user', start_scn)
```

##### 3.2.3.2 为YStream服务新增解析表名和模式

```sql
DBMS_YSTREAM_ADM.ADD_TABLES(
server_name  IN VARCHAR(64),
table_names  IN VARCHAR(4096),
schemas    IN VARCHAR(4096));
```



**注意：**这里添加的表名和模式要跟connector要捕获的表名和模式相同。

##### 3.2.3.3 为YStream服务设置参数

```sql
DBMS_YSTREAM_ADM.SET_PARAMETER(
server_name  IN VARCHAR(64),
parameter   IN VARCHAR(64),
value     IN VARCHAR(64));
```

SET_PARAMETER函数用于为已有服务设置参数，对应服务必须处于允许执行当前操作的状态（可通过查询V$YSTREAM_SERVER视图获取服务的状态）。

##### 3.2.3.4 启动YStream服务

```sql
DBMS_YSTREAM_ADM.START(server_name  IN VARCHAR(64));
```

### 3.3 部署debezium connector yashandb 

1. 下载debezium-connector-yashandb-2.4.2.0.jar包

2. 下载YashanDB Jdbc依赖

3. 下载YashanDB YStream 依赖

4. 下载debezium相关依赖

   1. 下载路径：https://repo1.maven.org/maven2/io/debezium/debezium-connector-oracle/2.4.2.Final/debezium-connector-oracle-2.4.2.Final-plugin.tar.gz
   
5. 在kafka安装目录创建文件夹（plugins/debezium-connector-yashandb）

6. 将第4步下载的gz包解压到目录plugins/debezium-connector-yashandb中，并将其中的debezium-connector-oracle-2.4.2.Final.jar删除掉。

7. 将以下文件放入目录plugins/debezium-connector-yashandb中

   1. debezium-connector-yashandb-2.4.2.0.jar
   2. YashanDB jdbc依赖
   4. YashanDB YStream依赖

8. 将绝对路径（/kafkahome/plugin/debezium-connector-yashandb）添加到Kafka connect的配置文件的plugin.path

9. 重启Kafka Connect 进程

   ```
   bin/connect-distributed.sh config/connect-distributed.properties
   ```

10. 配置debezium YashanDB connector, 如下：

   ```json
   
   {
       "name": "yashandb-connector", 
       "config": {
           "connector.class" : "io.debezium.connector.yashandb.YashanDBConnector", 
           "database.hostname" : "<YashanDB_IP_ADDRESS>", 
           "database.port" : "1688", 
           "database.user" : "SYS", 
           "database.password" : "Cod-2022",  
           "database.dbname" : "TEST", 
           "topic.prefix" : "my_topic", 
           "database.url" : "jdbc:yasdb://<YashanDB_IP_ADDRESS>:1688/SYS", 
           "table.include.list" : "TEST.TAB01",
           "database.ystream.server.name" : "server1",  
           "lob.enabled" : "true",
           "schema.history.internal.kafka.bootstrap.servers" : "kafka:9092",
           "schema.history.internal.kafka.topic": "schema-changes.inventory" 
       }
   }
   ```

11. 启动任务即可，恭喜安装完成。

## 4. 权限管理

| 权限授予                                         | 说明                    |
| :----------------------------------------------- | :---------------------- |
| GRANT CREATE SESSION TO *username*;              | 创建会话所需权限        |
| GRANT SELECT ON V_$DATABASE TO *username*;       | 查询闪回scn所需权限     |
| GRANT SELECT ON V_$TRANSACTION TO *username*;    | 查询活跃事务所需权限    |
| GRANT SELECT ON V_$YSTREAM_SERVER TO *username*; | 查询YStream服务所需权限 |
| GRANT SELECT ANY TABLE TO *username*;            | 待迁移表的查询权限      |
| GRANT FLASHBACK ANY TABLE TO *username*;         | 待迁移表的闪回查询权限  |
| GRANT YSTREAM_CAPTURE TO *username*;             | YStream使用所需权限     |

## 5. 连接器属性

| 属性                            | 默认值                                         | 描述                                                         |
| :------------------------------ | :--------------------------------------------- | :----------------------------------------------------------- |
| name                            | No default                                     | 连接器的唯一名称。尝试使用相同名称再次注册将失败。（所有Kafka Connect连接器都需要此属性。） |
| connector.class                 | No default                                     | 连接器的Java类的名称。在YashanDB连接器中始终使用值“io.debezium.connector.oracle”。 |
| database.hostname               | No default                                     | YashanDB数据库服务的IP地址或者hostname。                     |
| database.port                   | No default                                     | YashanDB数据库服务的端口。                                   |
| database.user                   | No default                                     | 连接YashanDB数据库的用户名                                   |
| database.password               | No default                                     | 连接YashanDB数据库的用户名的密码                             |
| database.dbname                 | No default                                     | YashanDB数据库的名称                                         |
| database.url                    | No default                                     | YashanDB数据库的JDBC URL。                                   |
| database.ystream.server.name    | No default                                     | YashanDB数据库的YStream服务名称，请指定一个唯一值，该值还未存在在YashanDB数据库的select SERVER_NAME from V_$YSTREAM_SERVER中，连接器将自动以该名称来创建YStream server。 |
| ystream.blocking.queue.size     | 128                                            | YStream客户端内置阻塞队列的长度，获取增量逻辑日志时直接从该队列获取，默认值为128。 |
| ystream.poll.timeout            | 10                                             | 从阻塞队列中获取下一个结果的超时时间（单位：秒），默认值为10。 |
| ystream.client.response.timeout | 60                                             | YStream服务端等待YStream客户端响应的最长时间（单位：秒），默认值为60。 |
| topic.prefix                    | No default                                     | 主题前缀，为连接器从中捕获更改的Oracle数据库服务器提供命名空间。您设置的值将用作连接器发出的所有Kafka主题名称的前缀。指定一个在Debezium环境中的所有连接器中唯一的主题前缀。以下字符有效：字母数字字符、连字符、点和下划线。注意：不要更改此属性的值。如果更改名称值，重新启动后，连接器将向名称基于新值的主题发出后续事件，而不是继续向原始主题发出事件。连接器也无法恢复其数据库架构历史主题。 |
| snapshot.mode                   | initial                                        | 指定连接器用于对捕获的表进行快照的模式。您可以设置以下值：**always**快照包括捕获的表的结构和数据。指定此值以在每个连接器开始时用捕获的表中的完整数据表示填充主题。**initial**快照包括捕获的表的结构和数据。指定此值以使用捕获表中数据的完整表示填充主题。如果快照成功完成，则下次连接器启动时不会再次执行快照。**initial_only**快照包括捕获的表的结构和数据。连接器执行初始快照，然后停止，不处理任何后续更改。**schema_only**已弃用，请参阅no_data。**no_data**快照仅包括捕获的表的结构。如果希望连接器仅捕获快照后发生的更改的数据，请指定此值。**schema_only_recovery**已弃用，请参阅recovery。**recovery**这是已捕获更改的连接器的恢复设置。重新启动连接器时，此设置可恢复损坏或丢失的数据库架构历史主题。您可以定期设置它来“清理”一个意外增长的数据库模式历史主题。数据库架构历史主题需要无限保留。请注意，只有在保证自连接器之前关闭的时间点和拍摄快照的时间点以来没有发生架构更改的情况下，才能安全使用此模式。 快照完成后，连接器将继续从数据库的重做日志中读取更改事件，除非snapshot.mode配置为initial_only。**when_needed**连接器启动后，只有在检测到以下情况之一时，它才会执行快照： 它无法检测到任何主题偏移。 以前记录的偏移量指定了服务器上不可用的日志位置。**configuration_based**使用此选项，您可以通过一组前缀为“snapshot.mode.configration.based”的连接器属性来控制快照行为。**custom**连接器根据[snapshot.mode.custom.name](http://snapshot.mode.custom.name/)属性指定的实现执行快照，该属性定义了io.debezium.spi.snapshot的自定义实现。快照界面。 **有关更多信息，请参阅 [table of `snapshot.mode` options](https://debezium.io/documentation/reference/2.7/connectors/oracle.html#oracle-connector-snapshot-mode-options).。** |
| schema.include.list             | No default                                     | 一个可选的逗号分隔的正则表达式列表，与您想要捕获更改的模式名称相匹配。任何未包含在schema.include.list中的模式名称都将被排除在捕获其更改之外。默认情况下，所有非系统模式的更改都会被捕获。为了匹配模式的名称，Debezium应用您指定为锚定正则表达式的正则表达式。也就是说，指定的表达式与模式的整个名称字符串相匹配；它与架构名称中可能存在的子字符串不匹配。 如果在配置中包含此属性，请不要同时设置schema.exclude.list属性。 |
| schema.exclude.list             | No default                                     | 一个可选的逗号分隔的正则表达式列表，与您不想捕获更改的模式名称相匹配。任何名称未包含在schema.exclude.list中的模式都会捕获其更改，但系统模式除外。为了匹配模式的名称，Debezium应用您指定为锚定正则表达式的正则表达式。也就是说，指定的表达式与模式的整个名称字符串相匹配；它与架构名称中可能存在的子字符串不匹配。 如果在配置中包含此属性，请不要设置`schema.include.list`属性。 |
| table.include.list              | No default                                     | 一个可选的逗号分隔的正则表达式列表，与要捕获的表的完全限定表标识符相匹配。设置此属性后，连接器仅捕获指定表中的更改。每个表标识符使用以下格式：<schema_name>.<table_name>默认情况下，连接器监视每个捕获的数据库中的每个非系统表。 为了匹配表的名称，Debezium应用您指定为锚定正则表达式的正则表达式。也就是说，指定的表达式与表的整个名称字符串相匹配；它与表名中可能存在的子字符串不匹配。 如果在配置中包含此属性，请不要同时设置table.exclude.list属性。 |
| table.xclude.list               | No default                                     | 一个可选的逗号分隔的正则表达式列表，用于匹配要从监视中排除的表的完全限定表标识符。连接器从排除列表中未指定的任何表中捕获更改事件。使用以下格式指定每个表的标识符：<schema_name>.<table_name>为了匹配表的名称，Debezium应用您指定为锚定正则表达式的正则表达式。也就是说，指定的表达式与表的整个名称字符串相匹配；它与表名中可能存在的子字符串不匹配。 如果在配置中包含此属性，请不要同时设置table.include.list属性。 |
| max.batch.size                  | 2048                                           | 一个正整数值，指定此连接器每次迭代期间要处理的每批事件的最大大小。 |
| max.queue.size                  | 9182                                           | 正整数值，指定阻塞队列可以容纳的最大记录数。当Debezium读取数据库中的事件流时，它会在将事件写入Kafka之前将其放置在阻塞队列中。在连接器接收消息的速度快于将消息写入Kafka的速度的情况下，或者当Kafka不可用时，阻塞队列可以为从数据库读取更改事件提供背压。当连接器定期记录偏移量时，队列中保存的事件将被忽略。始终将max.queue.size的值设置为大于max.batch.size的数值。 |
| max.queue.size.in.bytes         | 0（disabled）                                  | 一个长整数值，指定阻塞队列的最大容量（以字节为单位）。默认情况下，不会为阻塞队列指定卷限制。要指定队列可以消耗的字节数，请将此属性设置为正长值。 如果还设置了max.queue.size，则当队列大小达到任一属性指定的限制时，将阻止对队列的写入。例如，如果将max.queue.size=1000，并将[max.queue.size.in](http://max.queue.size.in/).bytes设置为5000，则在队列包含1000条记录或队列中的记录量达到5000字节后，将阻止向队列写入。 |
| poll.interval.ms                | 500 （0.5 second）                             | 正整数值，指定连接器在每次迭代期间应等待新更改事件出现的毫秒数。 |
| snapshot.fetch.size             | 10000                                          | 指定在snapshot快照时从每个表一次读取的最大行数。连接器以指定大小的多批读取表内容。 |
| query.fetch.size                | 10000                                          | JDBC 查询的fetch size。                                      |
| lob.enabled                     | false                                          | 控制是否在更改事件中发出大对象（CLOB或BLOB等等）列值。默认情况下，更改事件具有较大的对象列，但这些列不包含值。在处理和管理大型对象列类型和有效负载时会有一定的开销。要捕获大型对象值并在更改事件中对其进行序列化，请将此选项设置为true。 |
| decimal.handling.mode           | precise                                        | 指定连接器应如何处理NUMBER、DECIMAL和NUMERIC列的浮点值。您可以设置以下选项之一：<br>`precise` *(default)*:使用java.math精确表示值。以二进制形式在更改事件中表示的BigDecimal值(注意：此种形式对负scale的浮点值支持程度不够，建议使用string选项)。<br>`double`:使用双精度值表示值。使用双精度值更容易，但可能会导致精度损失。<br>`string`：将值编码为格式化字符串。使用字符串选项更容易使用，但会导致有关真实类型的语义信息丢失。 |
| unavailable.value.placeholder   | `__debezium_unavailable_value`                 | 指定连接器提供的常数，以指示原始值未更改且不是由数据库提供的。比如，lob未能获取到，就使用其代替。 |
| skipped.operations              | t                                              | 您希望连接器在流式传输过程中跳过的操作类型的逗号分隔列表。您可以配置连接器以跳过以下类型的操作：c（插入/创建）u（更新）d（删除）t（截断）默认情况下，只跳过截断操作。 |
| signal.data.collection          | No default value                               | 用于向连接器发送信号的数据采集的完全限定名称。使用以下格式指定集合名称： <databaseName>.<schemaName>.<tableName> |
| signal.enabled.channels         | source                                         | 为连接器启用的信号通道名称列表。默认情况下，以下频道可用：sourcekafkafilejmxOptionally, you can also implement a [custom signaling channel](https://debezium.io/documentation/reference/2.7/configuration/signalling.html#debezium-signaling-enabling-custom-signaling-channel). |
| notification.enabled.channels   | No default                                     | 为连接器启用的通知通道名称列表。默认情况下，以下频道可用：sinklogjmxOptionally, you can also implement a [custom notification channel](https://debezium.io/documentation/reference/2.7/configuration/notification.html#debezium-notification-custom-channel). |
| incremental.snapshot.chunk.size | 1024                                           | 在增量快照块期间，连接器获取并读入内存的最大行数。增加块大小可以提供更高的效率，因为快照运行的快照查询更少，但查询的大小更大。然而，较大的块大小也需要更多的内存来缓冲快照数据。将块大小调整为在您的环境中提供最佳性能的值。 |
| topic.naming.strategy           | `io.debezium.schema.SchemaTopicNamingStrategy` | 用于确定数据更改、模式更改、事务、心跳事件等的主题名称的类名，默认为SchemaTopicNamingStrategy。 |
| topic.delimiter                 | .                                              | 指定主题名称的分隔符，默认为. 。                             |
| snapshot.max.threads            | 1                                              | 指定连接器在执行初始快照时使用的线程数。要启用并行初始快照，请将属性设置为大于1的值。在并行初始快照中，连接器同时处理多个表。这个功能正在孵化。 |
| converters                      | No default                                     | 配置Debezium的自定义转换器                                   |
| <converter_name>.type           | No default                                     | 配置Debezium的自定义转换器的类名                             |
| <converter_name>.<param_name>   | No default                                     | 自定义转换器的配置，配置信息根据转换器的使用方式来设置       |

其他参数请参考：[Debezium Connector for Oracle :: Debezium Documentation](https://debezium.io/documentation/reference/2.7/connectors/oracle.html#oracle-connector-properties)

## 6. 数据类型映射

当 Debezium Oracle 连接器检测到表行的值发生更改时，它会发出表示该更改的更改事件。 每个更改事件记录的结构与原始表的结构相同，事件记录包含每个列值的字段。 表列的数据类型决定了连接器如何在更改事件字段中表示列的值，如以下各节中的表所示。

对于表中的每一列，Debezium 将源数据类型映射到literal 文本类型。

文本类型

使用以下 Kafka Connect 架构类型之一描述如何按字面表示值：INT8、INT16、INT32、INT64、FLOAT32、FLOAT64、BOOLEAN、STRING、BYTES、ARRAY、MAP、STRUCT。

| YashanDB data type     | Literal type (schema type)                     |
| :--------------------- | :--------------------------------------------- |
| CHAR[(M)]              | STRING                                         |
| NCHAR[(M)]             | STRING                                         |
| VARCHAR[(M)]           | STRING                                         |
| NVARCHAR[(M)]          | STRING                                         |
| BLOB                   | BYTES                                          |
| CLOB                   | STRING                                         |
| NCLOB                  | STRING                                         |
| RAW                    | BYTES                                          |
| TINYINT                | INT8                                           |
| SMALLINT               | INT16                                          |
| INT                    | INT32                                          |
| BIGINT                 | INT64                                          |
| FLOAT                  | FLOAT32                                        |
| DOUBLE                 | FLOAT64                                        |
| NUMBER                 | `BYTES` / `INT8` / `INT16` / `INT32` / `INT64` |
| BIT(1)                 | BOOLEAN                                        |
| BIT(n)                 | BYTES                                          |
| BOOLEAN                | BOOLEAN                                        |
| DATE                   | INT64                                          |
| TIME                   | INT64                                          |
| TIMESTAMP              | INT64                                          |
| INTERVAL YEAR TO MOUTH | FLOAT64                                        |
| INTERVAL DAY TO SECOND | FLOAT64                                        |
| ROWID                  | STRING                                         |
| UROWID                 | STRING                                         |

## 7. 数据定制化转换

数据类型映射中，DATE、TIME、TIMESTAMP映射INT64，如果想定制成‘yyyy-MM-dd HH:mm:ss.SSSSSS’的形式。connector提供了三种转换器来定制化转换这三种类型的数据。

| CustomConverter转换器                                        | 参数            | 说明                                            |
| ------------------------------------------------------------ | --------------- | ----------------------------------------------- |
| io.debezium.connector.yashandb.converters.TimestampToStringConverter | format:数据格式 | 将Timestamp类型数据转换成定制化格式的字符串数据 |
| io.debezium.connector.yashandb.converters.DateToStringConverter | format:数据格式 | 将Date类型数据转换成定制化格式的字符串数据      |
| io.debezium.connector.yashandb.converters.TimeToStringConverter | format:数据格式 | 将Time类型数据转换成定制化格式的字符串数据      |

使用样例，在配置里填写以下

```
# 命名两个转换器，yashandb_timestamp_formatter转换器用于将TIMESTAMP类型数据转换，yashandb_date_formatter转换器用于将DATE类型数据转换
”converters“: "yashandb_timestamp_formatter,yashandb_date_formatter"
# yashandb_timestamp_formatter绑定成io.debezium.connector.yashandb.converters.TimestampToStringConverter类名
”yashandb_timestamp_formatter.type“：”io.debezium.connector.yashandb.converters.TimestampToStringConverter“
# timestamp数据格式化成yyyy-MM-dd HH:mm:ss.SSSSSS格式
”yashandb_timestamp_formatter.format.datetime“：”yyyy-MM-dd HH:mm:ss.SSSSSS“
# yashandb_date_formatter绑定成io.debezium.connector.yashandb.converters.DateToStringConverter类名
”yashandb_date_formatter.type“：”io.debezium.connector.yashandb.converters.DateToStringConverter“
# DATE数据格式化成yyyy-MM-dd格式
”yashandb_date_formatter.format.date“：”yyyy-MM-dd“
```

#### 如何定制化编写一个转换器？

参考连接https://debezium.io/documentation/reference/2.4/development/converters.html#custom-converters

下面的示例显示了实现该接口的 Java 类的转换器实现io.debezium.spi.converter.CustomConverter：

    public interface CustomConverter<S, F extends ConvertedField> {
    
        @FunctionalInterface
        interface Converter {  
            Object convert(Object input);
        }
    
        public interface ConverterRegistration<S> { 
            void register(S fieldSchema, Converter converter); 
        }
    
        void configure(Properties props);
    
        void converterFor(F field, ConverterRegistration<S> registration); 
    }


​    
- interface Converter 接口：将数据从一种类型转换为另一种类型的函数。
- interface ConverterRegistration<S>接口：注册转换器的回调。
- register(S fieldSchema, Converter converter)：为当前字段注册给定的架构和转换器。不应针对同一字段多次调用。
- converterFor(F field, ConverterRegistration<S> registration)：注册自定义值和模式转换器以供特定字段使用。

##### 自定义转换器方法

接口的实现CustomConverter必须包括以下方法：

1. configure()
   1. 将连接器配置中指定的属性传递给转换器实例。该configure方法在连接器初始化时运行。您可以将转换器与多个连接器一起使用，并根据连接器的属性设置修改其行为。
   2. 该configure方法接受以下参数：
      1. props
         包含要传递给转换器实例的属性。每个属性指定用于转换特定类型列的值的格式。
2. converterFor()
   1. 注册转换器以处理数据源中的特定列或字段。Debezium 调用该converterFor()方法以提示转换器调用转换registration。该converterFor方法对每一列运行一次。
   2. 该方法接受以下参数：
      1. field
         传递有关所处理字段或列的元数据的对象。列元数据可以包括列或字段的名称、表或集合的名称、数据类型、大小等。
      2. registration
         io.debezium.spi.converter.CustomConverter.ConverterRegistration提供目标架构定义和用于转换列数据的代码的类型的对象。registration当源列与转换器应处理的类型匹配时，转换器将调用该参数。调用该register方法为架构中的每个列定义转换器。架构使用 Kafka Connect API 表示SchemaBuilder。将来，将添加独立的架构定义 API。

##### Debezium 自定义转换器示例

下面的示例实现了一个简单的转换器，它执行以下操作：

- 运行该configure方法，该方法根据schema.name连接器配置中指定的属性值配置转换器。转换器配置特定于每个实例。


- 运行该converterFor方法，该方法注册转换器来处理数据类型设置为的源列中的值isbn。
  - STRING根据为属性指定的值识别目标架构schema.name。
  - 将源列中的 ISBN 数据转换为String值。

示例 1. 一个简单的自定义转换器

    public static class IsbnConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    
        private SchemaBuilder isbnSchema;
    
        @Override
        public void configure(Properties props) {
            isbnSchema = SchemaBuilder.string().name(props.getProperty("schema.name"));
        }
    
        @Override
        public void converterFor(RelationalColumn column,
                ConverterRegistration<SchemaBuilder> registration) {
    
            if ("isbn".equals(column.typeName())) {
                registration.register(isbnSchema, x -> x.toString());
            }
        }
    }
##### Debezium 和 Kafka Connect API 模块依赖关系

自定义转换器 Java 项目对 Debezium API 和 Kafka Connect API 库模块具有编译依赖项。这些编译依赖项必须包含在您的项目中pom.xml，如以下示例所示：

```
<dependency>
    <groupId>io.debezium</groupId>
    <artifactId>debezium-api</artifactId>
    <version>${version.debezium}</version> 
</dependency>
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>connect-api</artifactId>
    <version>${version.kafka}</version> 
</dependency>
```

- ${version.debezium}表示 Debezium 连接器的版本，根据connector支持的版本，这里应该是2.4.2.Final。
- ${version.kafka}代表您环境中的 Apache Kafka 版本。

## 8. 限制

1. 受限于YStream，不支持自定义数据类型、XMLTYPE、 JSON数据类型。

## 8. Q&A

#### 8.1 报错：YashanDB does not yet have the YStream server ‘serverxx’ or check option 'database.ystream.server.name' if the parameters are filled in correctly. Please create and configure the YStream server, refer to the link 'xxx'.

A: 填写的参数'database.ystream.server.name'对应的YStream server 不存在在YashanDB数据库中，请创建相关的YStream server ，参考链接：[DBMS_YSTREAM_ADM | YashanDB Doc (yasdb.com)](https://cod-doc.yasdb.com/yashandb/23.3/zh/开发手册/PL参考手册/内置高级包/DBMS_YSTREAM_ADM.html)

#### 8.2 报错：YashanDB YStream server status is xxx. Please execute 'DBMS_YSTREAM_ADM.START( server_name IN VARCHAR(64) );' start YStream server。

A: 填写的参数'database.ystream.server.name'对应的YStream server 处于非运行状态或者非启动状态，请先在数据库中执行DBMS_YSTREAM_ADM.START( server_name IN VARCHAR(64) )启动该YStream服务。例如：

```
exec DBMS_YSTREAM_ADM.START('server1');
```

#### 8.3 Decimal数值同步到Kafka后，为什么序列化出来数据出错呢？

A: debezium会将负scale的Decimal会进行特殊处理，建议使用参数`decimal.handling.mode`=string来规避。

#### 8.4 DATE\TIME\TIMESTAMP数值同步到Kafka后，为什么是时间戳的形式，而不是‘yyyy-MM-dd HH:mm:ss.SSSSSS’的形式？

A: debezium的默认处理方式是将时间类型映射到INT64，如果需要映射到固定格式数据，可参考《第七点 数据定制化转换》。

#### 8.5 YashanDB Connector是否支持断点续传，任务停止或者失败后，是否能从上一个提交的点位开始捕获增量数据？

A：YashanDB Connector支持断点续传，基于Kafka的两阶段提交，任务停止或者失败后，上一个提交成功的日志点位会记录在Kafka的元数据信息中，恢复任务后，Connector会获取到最后一个提交成功的日志点位，并从该点位开始抓取数据，确保数据精确一次同步到Kafka的主题上。

#### 8.6 YashanDB Connector查看运行的任务日志，连接器会捕获整个库的表的元数据结构，有没有手段让连接器只捕获配置信息（schema.include.list和table.include.list）里的元数据结构？

A：Debezium会默认捕获整个库的表结构，可以在任务配置中设置schema.history.internal.store.only.captured.tables.ddl=true，即可只捕获配置信息（schema.include.list和table.include.list）里的表结构。

#### 8.7 删除任务重建后，再次启动后没有捕获表的元数据结构，比如在运行日志中没有看到”Capturing structure of table“，这是为什么呢？

A：debezium提供schema.history.internal.kafka.topic=schema-changes.inventory这个配置会生成一个topic保存一份表的元数据，所以删除任务重建后，如果任务名称没有变，并且没有删除schema-changes.inventory主题，就不会重新再捕获元数据结构。

#### 8.8 文档上描述不支持自定义数据类型、XMLTYPE、 JSON数据类型，为什么XMLTYPE、 JSON数据还能正常同步到Kafka的主题上呢？

A：YashanDB Connector对于类型的支持情况来源于YashanDB YStream的支持范围，XMLTYPE、 JSON数据能正常同步，但是数据的正确性上无法保证，依赖于YStream的支持。

