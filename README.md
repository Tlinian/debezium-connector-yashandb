## YashanDB Debezium Connector

YashanDB Debezium Connector 是基于debezium用于打通同步YashanDB的全量数据和增量数据到Kafka。

### 编辑构建

先决条件：

- [Git](https://git-scm.com) 2.2.1 or later

- JDK 17 or later, e.g. [OpenJDK](http://openjdk.java.net/projects/jdk/)

- [Apache Maven](https://maven.apache.org/index.html) 3.8.4 or later

首先通过克隆Git存储库获取代码：

```
git clone https://git.yasdb.com/cod-noah/yas-kafka-connector.git
cd yas-debezium-connector
```

然后使用Maven构建代码：

```
mvn clean verify -Dquick
```

### 文档说明

有关YashanDB Debezium Connector 版本中的新功能，请参阅此文档。

[docs](./docs).

## LICENSE

The connector is under the [Apache License 2.0](../LICENSE.txt).