package com.spring.transaction.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
public class KafkaProducerConfigTrx {

    @Bean
    public ProducerFactory<String, String> stringKeyValueproducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, getTransactionId());
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60000);
        configProps.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 3000);
        configProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    protected String getTransactionId() {
        return UUID.randomUUID().toString();
    }

    @Bean(name = "strKafkaTransactionManager")
    public KafkaTransactionManager<String, String> kafkaTransactionManager() {
        return new KafkaTransactionManager<>(stringKeyValueproducerFactory());
    }

    @Bean("strKVkafkaTemplateTrx")
    public KafkaTemplate<String, String> kafkaTemplateStringKV() {
        return new KafkaTemplate<>(stringKeyValueproducerFactory());
    }
}
