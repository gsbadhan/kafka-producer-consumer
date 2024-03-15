package com.spring.avro.producer;

import com.avro.dto.User;
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
class SpringKafkaProducerAvroTestIT {
    @Autowired
    private SpringKafkaProducerAvro springKafkaProducer;

    @Before
    void setUp() {
    }

    @After
    void tearDown() {
    }

    @Test
    void sendMessageStringKV() throws InterruptedException {
        String topic = "users-avro";
        User user1 = User.newBuilder().setId(1001L).setName("testIT-1001").setAge(20).setCompany("abc").build();
        assertTrue(springKafkaProducer.sendMessageStringKV(topic, user1.getId().toString(), user1));
        Thread.sleep(5 * 1000);
        User user2 = User.newBuilder().setId(1002L).setName("testIT-1002").setAge(22).setCompany("abc").build();
        assertTrue(springKafkaProducer.sendMessageStringKV(topic, user2.getId().toString(), user2));

        Thread.sleep(5 * 1000);
        User user3 = User.newBuilder().setId(1003L).setName("testIT-1003").setAge(23).setCompany("abc").build();
        assertTrue(springKafkaProducer.sendMessageStringKV(topic, user3.getId().toString(), user3));
        Thread.sleep(5 * 1000);
    }
}
