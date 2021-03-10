import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerTest {
    public static void main(String[] args) {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "1");

// use producer for interacting with Apache Kafka
        Vertx vertx = Vertx.vertx();
        io.vertx.kafka.client.producer.KafkaProducer<String, String> producer = io.vertx.kafka.client.producer.KafkaProducer.create(vertx, config);
        for (int i = 0; i < 5; i++) {
            // only topic and message value are specified, round robin on destination partitions
            KafkaProducerRecord<String, String> record =
                    KafkaProducerRecord.create("kafkaTestTopic", "message_" + i);

            producer.write(record);
        }
    }
}
