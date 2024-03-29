## producer/consumer code samples e.g. plain integration, spring integration and spring transaction integration   


## Download and install Apache Kafka version : kafka_2.12-3.5.1


## start zookeeper
./zookeeper-server-start.sh ../config/zookeeper.properties &


## start kafka broker
./kafka-server-start.sh ../config/server.properties &


## start topic, exmaple dataitems
./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic dataitems


## list of topics
./kafka-topics.sh --list --bootstrap-server localhost:9092


## status of topic
./kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic dataitems


## push message into topic (console)
./kafka-console-producer.sh --broker-list localhost:9092 --topic dataitems


## pull message from topic (console)
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic dataitems


## for AVRO schema integration, download confluent platform and start schema-registry
./schema-registry-start ../etc/schema-registry/schema-registry.properties

## AVRO schema location
resources/avro/user-v1.avsc

## AVRO pojo location
target/generated-sources/avro/com/avro/dto/User.java

## verify AVRO schema
http://127.0.0.1:8081/subjects
http://127.0.0.1:8081/subjects/users-avro-value/versions
http://127.0.0.1:8081/subjects/users-avro-value/versions/1


