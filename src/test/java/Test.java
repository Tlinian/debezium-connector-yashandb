import org.apache.kafka.connect.cli.ConnectStandalone;

import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args) {
        Map<String, String> kafkaProps = new HashMap<>();
        // 指定broker（这里指定了2个，1个备用），如果你是集群更改主机名即可，如果不是只写运行的主机名
        kafkaProps.put("bootstrap.servers", "192.168.8.203:9092");
        kafkaProps.put("group.id", "CountryCounter"); // 消费者群组
        // 设置序列化（自带的StringSerializer，如果消息的值为对象，就需要使用其他序列化方式，如Avro ）
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("connection.url", "jdbc:yasdb://192.168.8.203:2688/SYS");
        kafkaProps.put("connection.user", "SYS");
        kafkaProps.put("connection.password", "Cod-2022");
        kafkaProps.put("table.whitelist", "SYS.TARGET_BOOLEAN_TAB");

        ConnectStandalone.main(new String[]{
                "debezium-connector-yashandb/src/test/resources/connect-standalone.properties",
                "debezium-connector-yashandb/src/test/resources/yashandb-connector-source.properties"
        });
    }
}
