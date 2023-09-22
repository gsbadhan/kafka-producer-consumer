package com.spring.transaction.producer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@RunWith(SpringRunner.class)
@DirtiesContext
class SpringKafkaProducerTrxTestIT {

    @Autowired
    private SpringKafkaProducerTrx springKafkaProducer;

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void sendMessageStringKV() throws InterruptedException {
        String topic = "dataitemsTrx";
        assertTrue(springKafkaProducer.sendMessageStringKV(topic, "1001", "testIT-1001"));
        Thread.sleep(5 * 1000);
        assertTrue(springKafkaProducer.sendMessageStringKV(topic, "1002", "testIT-1002"));
        Thread.sleep(5 * 1000);
        assertTrue(springKafkaProducer.sendMessageStringKV(topic, "1003", "testIT-1003"));
        Thread.sleep(5 * 1000);
    }
}