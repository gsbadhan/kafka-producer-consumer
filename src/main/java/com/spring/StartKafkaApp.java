package com.spring;

import com.spring.producer.SpringKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan(basePackages = {"com.spring"})
@Slf4j
public class StartKafkaApp implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(StartKafkaApp.class, args);
        log.info("kafka consumer/producer started..");
    }

    @Autowired
    SpringKafkaProducer springKafkaProducer;


    @Override
    public void run(String... args) throws Exception {
        Thread.sleep(10 * 1000);
        springKafkaProducer.sendMessageStringKV("dataitems", "1001", "testmsg1");
        Thread.sleep(5 * 1000);
        springKafkaProducer.sendMessageStringKV("dataitems", "1002", "testmsg2");
        Thread.sleep(5 * 1000);
        springKafkaProducer.sendMessageStringKV("dataitems", "1003", "testmsg3");
    }
}
