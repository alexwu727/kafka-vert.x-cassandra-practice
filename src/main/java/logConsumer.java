import com.datastax.oss.driver.api.core.cql.Row;
import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.CassandraClientOptions;
import io.vertx.cassandra.ResultSet;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Objects;
import java.util.Properties;

public class logConsumer {
    private final Properties props;
    Vertx vertx = Vertx.vertx();
    public logConsumer(String brokers, String groupId) {
        props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
    }

    public void consumeAndSend2Cassandra(String topic){
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, props);
        // set up Cassandra
        CassandraClientOptions options = new CassandraClientOptions()
                .addContactPoint("localhost", 9042)
                .setKeyspace("test_keyspace");
        options.dataStaxClusterBuilder().withLocalDatacenter("datacenter1");
        CassandraClient client = CassandraClient.create(vertx, options);

        // consume
        consumer.handler(record -> {
            // get table
            // TODO
            String table = "test_table";

            // get id
            String logData = record.value();
            String id = null;
            try {
                JSONObject logDataJson = new JSONObject(logData);
                id = logDataJson.getString("sender_id");
            } catch (JSONException err) {
                System.out.println(err);
            }
            String finalId = id;
            // If the table doesn't exist, create one.
            checkTable(client, table).onComplete(voidAsyncResult -> {
               if (voidAsyncResult.succeeded()){
                   // insert log data into Cassandra
                   String insertDataQueryString = String.format("INSERT INTO %s (id, log) VALUES ('%s', '%s')", table, finalId, logData);
                   System.out.println(insertDataQueryString);
                   client.execute(insertDataQueryString);
               }
            });
        });
        consumer.subscribe(topic);
    }

    public static Future<Void> checkTable(CassandraClient client, String table){
        Promise<Void> promise = Promise.promise();
        client.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'test_keyspace'", queryStream -> {
            if (queryStream.succeeded()) {
                ResultSet stream = queryStream.result();
                stream.all(listAsyncResult -> {
                    boolean tableExist = false;
                    for (Row row : listAsyncResult.result()) {
                        if (Objects.equals(row.getString("table_name"), table)) {
                            tableExist = true;
                        }
                    }
                    if (!tableExist) {
                        String createTableQueryString = String.format("CREATE TABLE %s (id varchar PRIMARY KEY, log varchar)", table);
                        client.execute(createTableQueryString, ar2 -> {
                            if (ar2.succeeded()) {
                                System.out.println("successfully create table " + table + ".");
                                promise.complete();
                            }
                        });
                    } else {
                        promise.complete();
                    }
                });
            }
        });
        return promise.future();
    }
    public static void main(String[] args) {
        logConsumer c = new logConsumer("localhost:9092", "kafkatestgroup");
        c.consumeAndSend2Cassandra("testTopic");
    }
}
