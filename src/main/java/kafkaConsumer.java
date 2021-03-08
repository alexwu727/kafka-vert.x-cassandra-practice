import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.util.Properties;

public class kafkaConsumer {
    private final Properties props;
    Vertx vertx = Vertx.vertx();
    public kafkaConsumer(String brokers, String groupId) {
        props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
    }
    public void consume(String topic){
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, props);

        consumer.handler(record -> System.out.println("Processing key=" + record.key() + ", value=" + record.value() +
                ", partition=" + record.partition() + ", offset=" + record.offset()));
        consumer.subscribe(topic);
    }

    public static void main(String[] args) {
        kafkaConsumer c = new kafkaConsumer("localhost:9092", "kafkatestgroup");
        c.consume("kafkaTestTopic");
    }
}
