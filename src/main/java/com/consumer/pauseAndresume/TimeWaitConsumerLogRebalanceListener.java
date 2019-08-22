package com.consumer.pauseAndresume;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeWaitConsumerLogRebalanceListener implements ConsumerRebalanceListener {
    private static final Logger        LOG = LoggerFactory.getLogger(TimeWaitConsumerLogRebalanceListener.class);
    private TimeWaitConsumer       consumer;
    private Collection<TopicPartition> previousPartitions;

    public TimeWaitConsumerLogRebalanceListener(TimeWaitConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOG.info("Kafka Rebalacing: revoked thread " + Thread.currentThread().toString() + " for partition " + partitions);
        if (partitions == null || partitions.isEmpty()) {
            previousPartitions = Collections.emptyList();
        } else {
            previousPartitions = new ArrayList<>(partitions);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (previousPartitions.isEmpty() && partitions.isEmpty()) {
            LOG.debug("Kafka Rebalacing: Empty partitions- assigned thread " + Thread.currentThread().toString() + " for partition " + partitions);

        } else if (previousPartitions.containsAll(partitions) && partitions.containsAll(previousPartitions)) {
            LOG.debug("Kafka Rebalacing: same set of partitons- assigned thread " + Thread.currentThread().toString() + " for partition " + partitions);

        } else {
            LOG.info("Kafka Rebalacing: assigned thread " + Thread.currentThread().toString() + " to new partitions " + partitions + " from previous partitions "
                    + previousPartitions);
            consumer.threadPoolShutdown(true);
            consumer.regenerateName();
        }

    }
}
