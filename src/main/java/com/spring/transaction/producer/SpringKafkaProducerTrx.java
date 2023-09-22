package com.spring.transaction.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class SpringKafkaProducerTrx {

    @Autowired
    @Qualifier("strKVkafkaTemplateTrx")
    private KafkaTemplate<String, String> kafkaTemplate;

    @Transactional
    public boolean sendMessageStringKV(String topic, String key, String data) {
        CompletableFuture future = kafkaTemplate.send(topic, key, data);
        try {
            SendResult result = (SendResult) future.get(1000, TimeUnit.MILLISECONDS);
            log.info("Trx producer ack={}", result);
            return true;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            //TODO: you may retry before throwing exception
            throw new RuntimeException(e);
        }
    }
}
