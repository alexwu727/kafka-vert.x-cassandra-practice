import com.datastax.oss.driver.api.core.cql.Row;
import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.CassandraClientOptions;
import io.vertx.cassandra.ResultSet;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Objects;
import java.util.Properties;

public class BaseLogConsumer {
    Vertx vertx = Vertx.vertx();
    private Properties props;
    JsonObject config;

    public Future<Void> consume(){
        Promise<Void> consumePromise = Promise.promise();
        // kafka consumer client
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, props);
        // cassandra client
        CassandraClient client = createCassandraClient();

        deployWorker().onComplete(voidAsyncResult -> {
            if (voidAsyncResult.succeeded()){
                EventBus eventBus = vertx.eventBus();
                consumer.handler(record -> {
                    String logData = record.value();
                    String table = config.getJsonObject("cassandraConfig").getString("table");
                    // id
                    // TODO
                    String id = null;
                    try {
                        JSONObject logDataJson = new JSONObject(logData);
                        id = logDataJson.getString("sender_id");
                    } catch (JSONException err) {
                        System.out.println(err);
                    }
                    String finalId = id;

                    eventBus.request("worker", logData, messageAsyncResult -> {
                        String logDataAfterProcessing = messageAsyncResult.result().body().toString();
                        // If the table doesn't exist, create one.
                        checkTable(client, table).onComplete(voidAsyncResult2 -> {
                            if (voidAsyncResult2.succeeded()){
                                // insert log data into Cassandra
                                String insertDataQueryString = String.format("INSERT INTO %s (id, log) VALUES ('%s', '%s')", table, finalId, logDataAfterProcessing);
                                System.out.println(insertDataQueryString);
                                client.execute(insertDataQueryString);
                            }
                        });
                    });
                });

                String topic = config.getJsonObject("kafkaConfig").getString("topic");
                consumer.subscribe(topic);
            } else {
                consumePromise.fail("failed");
            }
        });
        return consumePromise.future();
    }

    public CassandraClient createCassandraClient(){
        String contactPoint = config.getJsonObject("cassandraConfig").getString("contactPoint");
        int port = config.getJsonObject("cassandraConfig").getInteger("port");
        String keyspace = config.getJsonObject("cassandraConfig").getString("keyspace");
        String datacenter = config.getJsonObject("cassandraConfig").getString("datacenter");
        CassandraClientOptions options = new CassandraClientOptions()
                .addContactPoint(contactPoint, port)
                .setKeyspace(keyspace);
        options.dataStaxClusterBuilder().withLocalDatacenter(datacenter);
        CassandraClient client = CassandraClient.create(vertx, options);
        return client;
    }
    public Future<Void> deployWorker(){
        Promise<Void> deployWorkerPromise = Promise.promise();
        int instanceNum = config.getJsonObject("workerConfig").getInteger("instanceNum");
        int workerPoolSize = config.getJsonObject("workerConfig").getInteger("workerPoolSize");
        DeploymentOptions workerOpts = new DeploymentOptions()
                .setWorker(true)
                .setInstances(instanceNum)
                .setWorkerPoolSize(workerPoolSize);
        vertx.deployVerticle(new WorkerVerticle(), workerOpts, stringAsyncResult -> {
            if (stringAsyncResult.succeeded()){
                deployWorkerPromise.complete();
            } else {
                deployWorkerPromise.fail("deploy failed");
            }
        });
        return deployWorkerPromise.future();
    }
    public static Future<Void> checkTable(CassandraClient client, String table){
        Promise<Void> checkTablePromise = Promise.promise();
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
                                checkTablePromise.complete();
                            }
                        });
                    } else {
                        checkTablePromise.complete();
                    }
                });
            }
        });
        return checkTablePromise.future();
    }
    public void setProps(){
        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "kafkatestgroup");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");

    }
    public void setConfig(){
        config = new JsonObject();

        JsonObject kafkaConfig = new JsonObject();
        kafkaConfig.put("topic", "testTopic");
        config.put("kafkaConfig", kafkaConfig);

        JsonObject cassandraConfig = new JsonObject();
        cassandraConfig.put("contactPoint", "localhost");
        cassandraConfig.put("port", 9042);
        cassandraConfig.put("keyspace", "test_keyspace");
        cassandraConfig.put("datacenter", "datacenter1");
        cassandraConfig.put("table", "test_table");
        config.put("cassandraConfig", cassandraConfig);

        JsonObject workerConfig = new JsonObject();
        workerConfig.put("instanceNum", 1);
        workerConfig.put("workerPoolSize", 1);
        config.put("workerConfig", workerConfig);
    }

    public static void main(String[] args) {
        BaseLogConsumer c = new BaseLogConsumer();
        c.setConfig();
        c.setProps();
        c.consume();
    }
}
