package com.consumer;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SingleConsumerAcrossTopic {
    private SingleConsumerAcrossTopic() {
    }

    private static Properties                    properties                   = new Properties();
    private static KafkaConsumer<String, String> consumer                     = null;
    private static long                          pollingTime                  = 100;
    private static long                          MAX_RECORDS_TO_COMMIT_OFFSET = 10;
    static {
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "cg1");
        properties.put("zookeeper.session.timeout.ms", "500");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(properties);
    }

    public static void consume(List<String> topics) {
        System.out.println("consumerThread:" + Thread.currentThread().getId() + " started..");
        consumer.subscribe(topics);
        int numberOfMsgConsumed = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(pollingTime);
            if (records.isEmpty()) {
                System.out.println("consumerThread:" + Thread.currentThread().getId() + " no record found!!");
                continue;
            }

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

    public static void consume() {
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

    public static void close() {
        consumer.close();
    }
}
