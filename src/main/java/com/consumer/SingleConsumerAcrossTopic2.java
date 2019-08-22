package com.consumer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class SingleConsumerAcrossTopic2 {
    private Properties                    properties                   = new Properties();
    private KafkaConsumer<String, String> consumer                     = null;
    private long                          pollingTime                  = 100;
    private long                          MAX_RECORDS_TO_COMMIT_OFFSET = 10;

    public SingleConsumerAcrossTopic2() {
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "cg1");
        properties.put("zookeeper.session.timeout.ms", "500");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(properties);
    }

    public void consume(List<String> topics) throws InterruptedException {
        System.out.println("consumerThread:" + Thread.currentThread().getId() + " started..");
        consumer.subscribe(topics);
        while (true) {
            Map<TopicPartition, OffsetAndMetadata> partitionOffset = Collections.synchronizedMap(new HashMap<TopicPartition, OffsetAndMetadata>());

            System.out.println("consumerThread:" + Thread.currentThread().getId() + " consumer.poll");
            ConsumerRecords<String, String> records = consumer.poll(pollingTime);
            if (records.isEmpty()) {
                System.out.println("consumerThread:" + Thread.currentThread().getId() + " no record found!!");
                Thread.sleep(1000);
                continue;
            }
            System.out.println("consumerThread:" + Thread.currentThread().getId() + " records " + records.count());
            System.out.println("consumerThread:" + Thread.currentThread().getId() + " partitions " + records.partitions());

            for (TopicPartition topicPartition : records.partitions()) {
                for (ConsumerRecord<String, String> record : records.records(topicPartition)) {
                    if (record == null || record.key() == null || record.value() == null) {
                        System.out.println("consumerThread:" + Thread.currentThread().getId() + " empty record " + topicPartition);
                        continue;
                    }
                    System.out.println("consumerThread:" + Thread.currentThread().getId() + " msg [" + record.value() + "] from partition [" + record.partition() + "]");

                    if (topicPartition.partition() % 2 == 0) {
                        System.out.println("consumerThread:" + Thread.currentThread().getId() + " digest [" + record.value() + "] from partition [" + record.partition() + "]");
                        partitionOffset.put(topicPartition, new OffsetAndMetadata(record.offset() + 1, String.valueOf(Thread.currentThread().getId())));
                    } else {
                        consumer.seek(topicPartition, record.offset());
                    }
                }
            }

            if (!partitionOffset.isEmpty()) {
                System.out.println("consumerThread:" + Thread.currentThread().getId() + " commitSync partitionOffset " + partitionOffset);
                consumer.commitSync(partitionOffset);
            }
        }

    }

    public void consume() {
        Collection<String> topics = consumer.listTopics().keySet();
        // remove below kafka's internal topics
        topics.remove("__consumer_offsets");
        topics.remove("_schema");

        System.out.println("all topic found:" + topics);
        consumer.subscribe(topics);
        int numberOfMsgConsumed = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(pollingTime);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("msg [" + record.value() + "] from partition [" + record.partition() + "]");
                ++numberOfMsgConsumed;
            }
            if (numberOfMsgConsumed >= MAX_RECORDS_TO_COMMIT_OFFSET) {
                numberOfMsgConsumed = 0;
                consumer.commitAsync();
            }
        }
    }

    public void close() {
        consumer.close();
    }
}
