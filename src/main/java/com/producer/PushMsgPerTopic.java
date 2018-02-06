package com.producer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class PushMsgPerTopic {

	private PushMsgPerTopic() {

	}

	private static Properties props = new Properties();
	private static Producer<String, String> producer = null;

	static {
		// broker IPs
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);
	}

	// send data to topic, no need to tell partition
	public static void send(final String topic, List<String> data) {
		for (String msg : data) {
			Future<RecordMetadata> status = producer
					.send(new ProducerRecord<String, String>(topic, String.valueOf(msg.hashCode()), msg));
			try {
				RecordMetadata metadata = status.get(200, TimeUnit.MILLISECONDS);
				System.out.println("msg [" + data + "] sent to partition[" + metadata.partition() + "]");
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}
		}
	}

	// send data to topic, partition info get from topic
	public static void sendAcrossPartitions(final String topic, int totalPartition, List<String> data) {
		int maxPartition = producer.partitionsFor(topic).size();
		for (int i = 0; i < data.size(); i++) {
			int partition = (i % maxPartition);
			Future<RecordMetadata> status = producer.send(new ProducerRecord<String, String>(topic, partition,
					String.valueOf(data.get(i).hashCode()), data.get(i)));
			try {
				RecordMetadata metadata = status.get(200, TimeUnit.MILLISECONDS);
				System.out.println("msg [" + data.get(i) + "] sent to partition[" + metadata.partition() + "]");
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}
		}
	}

	public static void close() {
		producer.close();
	}
}
