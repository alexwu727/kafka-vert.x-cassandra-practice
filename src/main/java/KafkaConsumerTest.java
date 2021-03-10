import io.vertx.core.Vertx;

import java.util.Properties;

public class KafkaConsumerTest {
    private final Properties props;
    Vertx vertx = Vertx.vertx();
    public KafkaConsumerTest(String brokers, String groupId) {
        props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
    }
    public void consume(String topic){
        io.vertx.kafka.client.consumer.KafkaConsumer<String, String> consumer = io.vertx.kafka.client.consumer.KafkaConsumer.create(vertx, props);

        consumer.handler(record -> System.out.println("Processing key=" + record.key() + ", value=" + record.value() +
                ", partition=" + record.partition() + ", offset=" + record.offset()));
        consumer.subscribe(topic);
    }

    public static void main(String[] args) {
        KafkaConsumerTest c = new KafkaConsumerTest("localhost:9092", "kafkatestgroup");
        c.consume("kafkaTestTopic");
    }
}
