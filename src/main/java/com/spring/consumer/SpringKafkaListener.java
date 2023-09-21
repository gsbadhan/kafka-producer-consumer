package com.spring.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SpringKafkaListener {

    @KafkaListener(beanRef = "strKVkafkaConsumer", topics = {"dataitems"}, groupId = "cg-1", containerFactory = "strKVkafkaConsumer")
    public void listenStringKV(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               @Header(KafkaHeaders.OFFSET) int offset, Acknowledgment ack) {
        log.info("received message={}, partition={}, offset={}, ack={}", message, partition, offset,ack);

        //TODO: process message

        //sending ack after consuming message or committing offset
        ack.acknowledge();
    }
}
