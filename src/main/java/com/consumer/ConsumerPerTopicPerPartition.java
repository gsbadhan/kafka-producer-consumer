package com.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class ConsumerPerTopicPerPartition {
	private ConsumerPerTopicPerPartition() {
	}

	private static Properties properties = new Properties();
	private static KafkaConsumer<String, String> consumer = null;
	private static long pollingTime = 100;
	private static long MAX_RECORDS_TO_COMMIT_OFFSET = 10;
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

	public static void consume(final String topic, final int partition) {
		Collection<TopicPartition> topics = new ArrayList<>(1);
		topics.add(new TopicPartition(topic, partition));
		consumer.assign(topics);
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
