
## Download and install Apache Kafka version : kafka_2.12-1.0.0


## start zookeeper
./zookeeper-server-start.sh ../config/zookeeper.properties &


## start kafka broker
./kafka-server-start.sh ../config/server.properties &


## start topic, exmaple dataitems
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic dataitems


## list of topics
./kafka-topics.sh --list --zookeeper localhost:2181


## status of topic
./kafka-topics.sh --describe --zookeeper localhost:2181 --topic events


## push message into topic (console)
./kafka-console-producer.sh --broker-list localhost:9092 --topic dataitems


## pull message from topic (console)
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic dataitems

