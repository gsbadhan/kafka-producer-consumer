package com.retry;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan(basePackages = {"com.retry"})
@EnableScheduling
@Slf4j
public class StartOfflineRetryApp {
    public static void main(String[] args) {
        SpringApplication.run(StartOfflineRetryApp.class, args);
        log.info("OfflineRetry consumer/producer started..");
    }

}
