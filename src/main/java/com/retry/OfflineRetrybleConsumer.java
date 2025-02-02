package com.retry;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * This class will cover offline retry and send message to DLQ on retry exhausted.
 *</p>
 *
 * * * Create below topics:
 * <p>./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic
 * test-retry-v1
 * <p>
 * ./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic
 * test-retry-dlq-v1
 * </p>
 *
 * * * Use Kafka CLI to send message
 * <p>
 * ./kafka-console-producer.sh --broker-list localhost:9092 --topic test-retry-v1
 * </p>
 */

@Component
@Slf4j
public class OfflineRetrybleConsumer {
    private static final String NEXT_RETRY_TIME_KEY = "NEXT_RETRY_TIME_KEY";
    private static final String LAST_RETRY_COUNT_KEY = "LAST_RETRY_COUNT";
    private static final Long MAX_RETRY_COUNT_VALUE = 2L;
    private static final String RETRY_TOPIC = "test-retry-v1";
    private static final String DLQ_TOPIC = "test-retry-dlq-v1";
    private static final Long RETRY_TIME = (2L * 60 * 1000);
    private static Producer<String, String> producer = getProducer();


    @Scheduled(initialDelay = 5, fixedRate = 60, timeUnit = TimeUnit.SECONDS)
    public void trigger() throws ExecutionException, InterruptedException {
        log.info("trigger started at={}", new Date());
        KafkaConsumer consumer = null;
        Set<TopicPartition> pausedTopicPartitions = new HashSet<>();
        try {
            consumer = getConsumer();
            while (true) {
                if (!pausedTopicPartitions.isEmpty()) {
                    log.info("pausedTopicPartitions={}", pausedTopicPartitions);
                    consumer.pause(pausedTopicPartitions);
                }
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                if (records.isEmpty()) {
                    log.info("no records found !!");
                    break;
                }
                for (ConsumerRecord<String, String> record : records) {
                    Long nextRetryTime = getHeaderValue(record, NEXT_RETRY_TIME_KEY);
                    Long lastRetryCount = getHeaderValue(record, LAST_RETRY_COUNT_KEY);
                    if (nextRetryTime != null && nextRetryTime > System.currentTimeMillis()) {
                        Set<TopicPartition> topicPartition = new HashSet<>(1);
                        topicPartition.add(new TopicPartition(RETRY_TOPIC, record.partition()));
                        consumer.pause(topicPartition);
                        log.info("partition paused for partition={},lastRetryCount={},nextRetryTime={},record={}",
                                topicPartition, lastRetryCount, new Date(nextRetryTime), record);
                        continue;
                    }
                    processRecord(consumer, record);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConsumer(consumer);
            log.info("consumer stopped for now !!");
        }
    }

    private void processRecord(KafkaConsumer consumer, ConsumerRecord<String, String> consumerRecord) {
        List headers = new ArrayList();
        try {
            Thread.sleep(5000);
            Long lastRetryCount = getHeaderValue(consumerRecord, LAST_RETRY_COUNT_KEY);
            if (lastRetryCount >= MAX_RETRY_COUNT_VALUE) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(DLQ_TOPIC, null, consumerRecord.key(),
                        consumerRecord.value(), headers);
                producer.send(producerRecord);
                consumer.commitSync();
                log.info("DLQ processRecord={}", consumerRecord);
            } else {
                Long retryCount = lastRetryCount + 1L;
                Long nextRetryTime = System.currentTimeMillis() + RETRY_TIME;
                headers.add(new RecordHeader(NEXT_RETRY_TIME_KEY, nextRetryTime.toString().getBytes(StandardCharsets.UTF_8)));
                headers.add(new RecordHeader(LAST_RETRY_COUNT_KEY, retryCount.toString().getBytes(StandardCharsets.UTF_8)));
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(RETRY_TOPIC, null, consumerRecord.key(),
                        consumerRecord.value(), headers);
                producer.send(producerRecord);
                consumer.commitSync();
                log.info("Next time retry processRecord={}", consumerRecord);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static KafkaConsumer<String, String> getConsumer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "testRetryGrp");
        properties.put("zookeeper.session.timeout.ms", "500");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("max.poll.records", "50");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        Collection<String> topics = new HashSet<>(1);
        topics.add(RETRY_TOPIC);
        consumer.subscribe(topics);
        return consumer;
    }

    private static void closeConsumer(KafkaConsumer consumer) {
        if (consumer != null) {
            consumer.close();
        }
    }

    private static Producer<String, String> getProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    private static void closeProducer(Producer<String, String> producer) {
        if (producer != null) {
            producer.close();
        }
    }

    private static Long getHeaderValue(ConsumerRecord record, String headerKey) {
        Iterable<Header> headers = record.headers();
        for (Header header : headers) {
            String key = header.key();
            if (key.equals(headerKey)) {
                byte[] valueBytes = header.value();
                Long value = Long.valueOf(new String(valueBytes, StandardCharsets.UTF_8));
                return value;
            }
        }
        return 0L;
    }
}
