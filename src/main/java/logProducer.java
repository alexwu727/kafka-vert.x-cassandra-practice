import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class logProducer {
    public static void main(String[] args) {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "1");

// use producer for interacting with Apache Kafka
        Vertx vertx = Vertx.vertx();
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config);
        KafkaProducerRecord<String, String> record =
                KafkaProducerRecord.create("testTopic", "{\"sender_id\": \"端午節.\"}");
        producer.write(record);
    }
}
