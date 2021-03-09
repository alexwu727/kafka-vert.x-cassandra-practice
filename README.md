# kafka-vert.x-cassandra
## Install 
* [Zookeeper](https://medium.com/@shaaslam/installing-apache-zookeeper-on-windows-45eda303e835)
* [Kafka](https://medium.com/@shaaslam/installing-apache-kafka-on-windows-495f6f2fd3c8)
* [Cassandra](https://phoenixnap.com/kb/install-cassandra-on-windows)
## Kafka
* Start Zookeeper  
Zookeeper和Kafka溝通是走2181  
`bin/zookeeper-server-start.sh config/zookeeper.properties`  

* Start a Kafka server  
Kafka和其client溝通是走9092  
`bin/kafka-server-start.sh config/server.properties`  

* Create Kafka topic  
`kafka-topics.bat --create --topic [topicName] --zookeeper localhost:2181
--partitions 1 --replication-factor 1`

* List all topic  
`kafka-topics.bat --list --zookeeper localhost:2181`

* Consume from a topic  
`kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic [topicName]`

* Produce to a topic  
`kafka-console-producer.bat --broker-list localhost:9092 --topic [topicName]`

Reference: https://blog.yowko.com/kafka-on-windows/

## Cassandra
* Set environment variable  
`Java, Python, Cassandra`
* Check environment variable  
`echo %JAVA_HOME%`  
`echo %CASSANDRA_HOME%`
* Start Cassandra  
`cassandra`
* Start cqlsh  
`cqlsh`  
Note: two issue often occur, java version and environment variable.  
Reference: https://phoenixnap.com/kb/install-cassandra-on-windows
