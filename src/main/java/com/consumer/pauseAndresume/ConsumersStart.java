package com.consumer.pauseAndresume;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumersStart implements ConsumerListenerThreads {
    private static final Logger    LOG                   = LoggerFactory.getLogger(ConsumersStart.class);

    private Processor              processor             = new Processor();
    private ExecutorService        consumerService;
    private List<TimeWaitConsumer> consumers;
    private int                    consumerPool          = -1;
    private int                    threadPoolPerConsumer = -1;
    private String                 topic                 = "testsms";
    private String                 group                 = "test-grp-sms11";

    @Override
    public void init() {
        if (consumerPool < 0) {
            consumerPool = 10;
        }

        if (threadPoolPerConsumer <= 0) {
            threadPoolPerConsumer = consumerPool;
        }

        if (consumerPool > 0 && threadPoolPerConsumer > 0) {
            startConsumers();
        }
    }

    @Override
    public int getConsumerPool() {
        return consumerPool;
    }

    @Override
    public void setConsumerPool(int consumerPool) {
        this.consumerPool = consumerPool;
    }

    @Override
    public int getThreadPoolPerConsumer() {
        return threadPoolPerConsumer;
    }

    @Override
    public void setThreadPoolPerConsumer(int threadPoolPerConsumer) {
        this.threadPoolPerConsumer = threadPoolPerConsumer;
    }

    @Override
    public void preDestroy() {
        if (consumers != null) {
            for (TimeWaitConsumer consumer : consumers) {
                consumer.activateShutdown();
            }
        }

    }

    @Override
    public void destroyNow() {
        if (consumers != null) {
            for (TimeWaitConsumer consumer : consumers) {
                consumer.shutdown();
            }
        }
        if (consumerService != null) {
            consumerService.shutdownNow();
            try {
                consumerService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.warn("Shutdown of ConsumerService reached limit time or failed for kafka customer: ", e);
            }
        }

    }

    @Override
    public void startConsumers() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1); //default 1
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 180000);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        LOG.info("Consumer details prop: " + props);
        addConsumers(props);

    }

    private void addConsumers(Properties props) {
        String serverName = group;
        try {
            serverName = InetAddress.getLocalHost().getHostName() + "-" + serverName;
        } catch (UnknownHostException e) {
            LOG.info("Consumer details client id failed to get hostname ", e);
        }
        consumerService = new ThreadPoolExecutor(consumerPool, consumerPool, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        consumers = new ArrayList<>(2 * consumerPool);
        TimeWaitConsumer consumer;
        for (int i = 0; i < 2 * consumerPool; i++) {
            consumer = new TimeWaitConsumer(props, Collections.singletonList(topic), threadPoolPerConsumer, processor, 10000L, serverName + "-" + i);
            consumerService.submit(consumer);
            consumers.add(consumer);
        }
    }

    @Override
    public void pauseAllConsumers() {
        if (consumers != null) {
            for (TimeWaitConsumer consumer : consumers) {
                consumer.pause();
            }
        }

    }

    @Override
    public void resumeAllConsumers() {
        if (consumers != null) {
            for (TimeWaitConsumer consumer : consumers) {
                consumer.resume();
            }
        }

    }

    private class NamingThreadFactory implements ThreadFactory {
        private final AtomicLong count = new AtomicLong(0l);
        private String           name;

        public NamingThreadFactory(String name) {
            this.name = name;

        }

        @Override
        public Thread newThread(Runnable r) {

            return new Thread(r, name + "-" + count.incrementAndGet());
        }

    }

}