package com.spring.producer;

import org.junit.After;
import org.junit.Before;
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
class SpringKafkaProducerTestIT {
    @Autowired
    private SpringKafkaProducer springKafkaProducer;

    @Before
    void setUp() {
    }

    @After
    void tearDown() {
    }

    @Test
    void sendMessageStringKV() throws InterruptedException {
        String topic = "dataitems";
        assertTrue(springKafkaProducer.sendMessageStringKV(topic, "1001", "testIT-1001"));
        Thread.sleep(5 * 1000);
        assertTrue(springKafkaProducer.sendMessageStringKV(topic, "1002", "testIT-1002"));
        Thread.sleep(5 * 1000);
        assertTrue(springKafkaProducer.sendMessageStringKV(topic, "1003", "testIT-1003"));
        Thread.sleep(5 * 1000);
    }
}