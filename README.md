


./zookeeper-start.sh ../config/zookeeper.properties &


./kafka-server-start.sh ../config/server.properties &


./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic events
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic dataitems

./kafka-topics.sh --list --zookeeper localhost:2181

./kafka-topics.sh --describe --zookeeper localhost:2181 --topic events


./producer.sh --broker-list localhost:9092 --topic dataitems

./consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic dataitems

