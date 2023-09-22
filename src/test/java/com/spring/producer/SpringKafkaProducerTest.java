package com.spring.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.jupiter.api.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Note: Not fully functional
 */
@SpringBootTest
@RunWith(SpringRunner.class)
@DirtiesContext
@Slf4j
class SpringKafkaProducerTest {
    @Autowired
    private SpringKafkaProducer springKafkaProducer;
    private static KafkaMessageListenerContainer<String, String> kafkaMessageListenerContainer;
    private static String TOPIC = "dataitems";
    private static String CONSUMER_GROUP = "di-testing-group";
    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, TOPIC);
    private static BlockingQueue<ConsumerRecord<String, String>> consumerRecords = new LinkedBlockingQueue<>();

    @BeforeAll
    static void setUp() {
        ContainerProperties containerProperties = new ContainerProperties(TOPIC);
        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(CONSUMER_GROUP, "false",
                embeddedKafka.getEmbeddedKafka());
        DefaultKafkaConsumerFactory<String, String> consumer = new DefaultKafkaConsumerFactory<>(consumerProperties);

        kafkaMessageListenerContainer = new KafkaMessageListenerContainer<>(consumer, containerProperties);
        kafkaMessageListenerContainer.setupMessageListener((MessageListener<String, String>) record -> {
            log.info("test listened message='{}'", record.toString());
            consumerRecords.add(record);
        });
        kafkaMessageListenerContainer.start();
        ContainerTestUtils.waitForAssignment(kafkaMessageListenerContainer,
                embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());
    }

    @AfterAll
    static void tearDown() {
        kafkaMessageListenerContainer.stop();
    }

    @Test
    void sendMessageStringKV() throws InterruptedException {
        String message = "testIT-1001";
        assertTrue(springKafkaProducer.sendMessageStringKV(TOPIC, "1001", message));

        ConsumerRecord<String, String> received = consumerRecords.poll(30, TimeUnit.SECONDS);
        assertNotNull(received);
        assertTrue(received.topic().equals(TOPIC));
        assertTrue(received.value().equals(message));
    }
}