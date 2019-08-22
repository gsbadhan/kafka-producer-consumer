package com.consumer.pauseAndresume;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


public class TimeWaitConsumer implements Runnable {
    private static final Logger                    LOG                = LoggerFactory.getLogger(TimeWaitConsumer.class);

    private final AtomicBoolean                    closed             = new AtomicBoolean(false);
    private KafkaConsumer<String, String>          consumer           = null;
    private ExecutorService                        service            = null;
    private String                                 clientId           = "client_";
    private int                                    noOfThreads        = 1;
    private Properties                             props;
    private List<String>                           topics;
    private String                                 name               = "";
    private GenericProcessor                       processor;
    private long                                   timeout            = Long.MAX_VALUE;
    private final AtomicBoolean                    rebalance          = new AtomicBoolean(false);
    private Map<TopicPartition, OffsetAndMetadata> partitionOffset    = Collections.synchronizedMap(new HashMap<TopicPartition, OffsetAndMetadata>());
    private long                                   minimumSleepTime   = 0L;
    private long                                   DEFAULT_SLEEP_TIME = 0L;

    public TimeWaitConsumer(Properties props, List<String> topics, int noOfThreads, GenericProcessor processor, long timeout, String clientId) {
        this.noOfThreads = noOfThreads;
        this.props = new Properties();
        this.props.putAll(props);
        this.topics = topics;
        this.processor = processor;
        this.timeout = timeout;
        this.clientId = clientId;
    }

    public void consume() {
        if (!closed.get()) {
            //assign partition to consumer using poll
            consumer.poll(timeout);
            //reseting offset to last committed offset in consumer position pointer
            resetOffsetPosition();
            regenerateName();
            //manual rebalance so setting flag off/ only in automatic rebalancing we clean resources on true
            rebalance.set(false);

        }
        Map<String, LinkedList<String>> batchRequest = new HashMap<>(this.noOfThreads);
        partitionOffset.clear();
        try {
            while (!closed.get()) {
              //resuming thread as further msgs should be fetch on poll
                silentResume();
                //wait
                holdOn();
                ConsumerRecords<String, String> records = consumer.poll(timeout);
                //pausing thread as no further msg should be fetch on poll
                silentPause();
                if (records == null || records.isEmpty())
                    continue;

                for (TopicPartition partition : records.partitions()) {
                    if (service == null) {
                        if (rebalance.get()) {
                            //removing pending batch as rebalanced call
                            batchRequest.clear();
                            //reseting rebalce status
                            rebalance.set(false);
                            resetOffsetPosition();
                            break;
                        }
                        if (name != null) {
                            regenerateName();
                        }
                        service = new ThreadPoolExecutor(noOfThreads, noOfThreads, 100, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>(), new NamingThreadFactory(consumer),
                                new CustomCallerRunsPolicy(consumer));
                    }
                    Set<Integer> partitionUnderTimewait = new HashSet<>(records.partitions().size() + 1);
                    for (ConsumerRecord<String, String> record : records.records(partition)) {
                        if (record == null || record.key() == null || record.value() == null || partitionUnderTimewait.contains(partition.partition()))
                            continue;

                        //check time to process record else mark as seek
                        String[] data = record.value().split("=");
                        long getTimeToConsume = Long.parseLong(data[0]);
                        if (getTimeToConsume >= System.currentTimeMillis()) {
                            consumer.seek(partition, record.offset());
                            partitionUnderTimewait.add(partition.partition());
                            if (partitionUnderTimewait.size() == 1) {
                                minimumSleepTime = getTimeToConsume;
                            } else {
                                minimumSleepTime = Math.min(minimumSleepTime, getTimeToConsume);
                            }
                            LOG.info("Message recieved but not consuming now for key for bucket sleeptime:" + minimumSleepTime + " : " + topics + " : "
                                    + record.key() + " value: " + record.value() + " partition: " + record.partition() + " offset: " + record.offset());
                            continue;
                        }

                        LOG.info("Message recieved key for bucket " + topics + " : " + record.key() + " value: " + record.value() + " partition: "
                                + record.partition() + " offset: " + record.offset());

                        if (batchRequest.size() < noOfThreads) {
                            if (batchRequest.containsKey(record.key())) {
                                addEntryToBatch(batchRequest, partition, record);
                            } else {
                                addNewEntryToBatch(batchRequest, partition, record);
                            }
                        } else if (batchRequest.size() >= noOfThreads) {
                            if (batchRequest.containsKey(record.key())) {
                                addEntryToBatch(batchRequest, partition, record);
                            } else {
                                //can proceed with same partition
                                processCommitAndClearBatch(batchRequest);
                                //continue processing for fair chance for all partitions
                                addNewEntryToBatch(batchRequest, partition, record);
                            }
                        }
                    }
                }
                if (batchRequest.size() > 0) {
                    processCommitAndClearBatch(batchRequest);
                }

            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            LOG.warn("Kafka Consumer WakeupException " + name, e);
            if (!closed.get())
                throw e;
        } catch (Throwable e) {
            // Ignore exception if closing
            LOG.error("Kafka Consumer Exception " + name, e);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            LOG.error("Kafka Consumer Exception " + name + " exception: " + sw.toString());
            if (e.getCause() != null) {
                sw = new StringWriter();
                pw = new PrintWriter(sw);
                e.getCause().printStackTrace(pw);
                LOG.error("Cause Kafka Consumer Exception " + name + " exception: " + sw.toString());
            }
            if (!closed.get())
                throw e;
        } finally {
            LOG.warn("Kafka Consumer is shuting down: " + name + " isClosed:" + closed.get());
            try {
                threadPoolShutdown();
            } catch (Exception e) {
            }

            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
        }
    }

    /**
     * wait for minimum sleep time across partitions
     */
    private void holdOn() {
        try {
            if (minimumSleepTime <= 0) {
                Thread.sleep(DEFAULT_SLEEP_TIME);
            } else {
                minimumSleepTime = (minimumSleepTime - System.currentTimeMillis());
                Thread.sleep(minimumSleepTime);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        minimumSleepTime = DEFAULT_SLEEP_TIME;
    }

    private void resetOffsetPosition() {
        for (TopicPartition partition : consumer.assignment()) {
            LOG.info("Consumer subscribed to :" + partition.partition() + " pos: " + consumer.position(partition) + " committed: " + consumer.committed(partition));
            if (consumer.committed(partition) != null)
                if (consumer.position(partition) >= consumer.committed(partition).offset()) {
                    consumer.seek(partition, consumer.committed(partition).offset());
                }
            LOG.info("New Consumer subscribed to :" + partition.partition() + " pos: " + consumer.position(partition) + " committed: " + consumer.committed(partition));

        }
    }

    private void processCommitAndClearBatch(Map<String, LinkedList<String>> batchRequest) {

        try {
            //this service can be null by rebalancing thread
            if (service != null) {
                Map<String, Future<Object>> futureResult = processor.process(service, batchRequest);

                Iterator<Entry<String, Future<Object>>> responseItr;
                Entry<String, Future<Object>> response;
                while (!futureResult.isEmpty()) {
                    responseItr = futureResult.entrySet().iterator();
                    while (responseItr.hasNext()) {

                        response = responseItr.next();
                        try {
                            response.getValue().get(5, TimeUnit.SECONDS);
                            if (response.getValue().isCancelled() || response.getValue().isDone()) {
                                responseItr.remove();
                            }
                            ConsumerRecords<String, String> emptyRecord = consumer.poll(0);
                            if (!(emptyRecord == null || emptyRecord.isEmpty())) {
                                LOG.error("{Internally Missed} Failed to pause consumer " + name + " for: " + consumer.assignment());
                                silentPause();
                            }
                        } catch (TimeoutException e) {
                            LOG.warn("TimeoutException occured in failed for event: " + response.getKey());
                            ConsumerRecords<String, String> emptyRecord = consumer.poll(0);
                            if (!(emptyRecord == null || emptyRecord.isEmpty())) {
                                LOG.error("{Internally Missed} Failed to pause consumer " + name + " for: " + consumer.assignment());
                                silentPause();
                            }
                            if (response.getValue().isDone() || response.getValue().isCancelled()) {
                                LOG.error("This should never happen for consumer " + name + " for: " + consumer.assignment());
                                responseItr.remove();
                            }
                        } catch (Exception e) {
                            LOG.error("Exception occured in failed for event: " + response.getKey(), e);
                            ConsumerRecords<String, String> emptyRecord = consumer.poll(0);
                            if (!(emptyRecord == null || emptyRecord.isEmpty())) {
                                LOG.error("{Internally Missed} Failed to pause consumer " + name + " for: " + consumer.assignment());
                                silentPause();
                            }
                            if (response.getValue().isDone() || response.getValue().isCancelled()) {
                                responseItr.remove();
                            }
                        }
                    }
                }
                LOG.info(" Commited:" + partitionOffset);
                consumer.commitSync(new HashMap<TopicPartition, OffsetAndMetadata>(partitionOffset));
            }
            batchRequest.clear();
            partitionOffset.clear();
        } catch (Exception e) {
            LOG.error("[processCommitAndClearBatch] Kafka Consumer Exception " + name + " exception: " + e, e);
            throw e;
        }
    }

    private void addNewEntryToBatch(Map<String, LinkedList<String>> batchRequest, TopicPartition partition, ConsumerRecord<String, String> record) {
        try {
            LinkedList<String> values = new LinkedList<>();
            values.add(record.value());
            batchRequest.put(record.key(), values);
            addRequestOffset(partition, record);
        } catch (Exception e) {
            LOG.error("[addNewEntryToBatch] Kafka Consumer Exception " + name + " exception: " + e, e);
            throw e;
        }
    }

    private void addEntryToBatch(Map<String, LinkedList<String>> batchRequest, TopicPartition partition, ConsumerRecord<String, String> record) {
        try {
            batchRequest.get(record.key()).add(record.value());
            addRequestOffset(partition, record);
        } catch (Exception e) {
            LOG.error("[addEntryToBatch] Kafka Consumer Exception " + name + " exception: " + e, e);
            throw e;
        }
    }

    private void addRequestOffset(TopicPartition partition, ConsumerRecord<String, String> record) {
        try {
            partitionOffset.put(partition, new OffsetAndMetadata(record.offset() + 1, clientId));
        } catch (Exception e) {
            LOG.error("[addRequestAndOffset] Kafka Consumer Exception " + name + " exception: " + e, e);
            throw e;
        }
    }

    public void threadPoolShutdown() {
        if (service != null) {
            partitionOffset.clear();
            service.shutdownNow();
            try {
                service.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.warn("Shutdown of thread pool reached limit time or failed for kafka customer: " + name, e);
            }
            service = null;

            LOG.info("Shutdown of thread pool for consumer during shutting down for " + name);
        }
    }

    public void threadPoolShutdown(boolean b) {
        //so that cunsumer.poll() should be call again
        this.rebalance.set(true);
        //stopping processing
        threadPoolShutdown();

    }

    public synchronized void regenerateName() {
        if (consumer != null) {
            StringBuilder str = new StringBuilder();
            for (TopicPartition tp : consumer.assignment()) {
                str.append("--");
                str.append(tp.topic());
                str.append("-");
                str.append(tp.partition());
            }
            name = str.toString();
        }

    }

    public KafkaConsumer<String, String> getKafkaConsumer() {
        return this.consumer;
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        LOG.info("setting close flag for consumer during shutting down for " + name);
        closed.set(true);
        //        threadPoolShutdown();
        if (consumer != null) {
            LOG.info("WAKING up consumer to shut down for " + name);
            consumer.wakeup();
        }
    }

    public void activateShutdown() {
        LOG.info("setting close flag for consumer before shut down for " + name);
        closed.set(true);
    }

    public void pause() {
        pause(true);
    }

    private void pause(boolean explicitPause) {
        if (consumer != null) {
            if (explicitPause) {
                LOG.info("going to pause consumer for " + name + " and assignment : " + consumer.assignment());
            }
            if (!(consumer.paused().containsAll(consumer.assignment()) && consumer.assignment().containsAll(consumer.paused()))) {
                consumer.pause(consumer.assignment());
            }
        }
    }

    private void silentPause() {
        try {
            pause(false);
        } catch (Exception e) {
            LOG.warn("[Internaly] Failed to pause consumer " + name + " for: " + consumer.assignment(), e);
        }
    }

    public void resume() {
        resume(true);
    }

    private void silentResume() {
        try {
            resume(false);
        } catch (Exception e) {
            LOG.warn("[Internaly] Failed to resume consumer " + name + " for: " + consumer.assignment(), e);
        }
    }

    private void resume(boolean explicitResume) {
        if (consumer != null) {
            if (explicitResume)
                LOG.info("going to resume consumer for " + name + " and assignment : " + consumer.assignment());
            consumer.resume(consumer.assignment());
        }
    }

    @Override
    public void run() {
        String topicStr = topics.get(0);
        GenericProcessor.generateRequestId(topicStr);
        if (clientId == null) {
            clientId = "client-" + MDC.get("requestId").replaceAll(":", "");
        } else {
            clientId = clientId + "-" + topicStr;
        }
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        if (service != null) {
            service.shutdown();
            service = null;
        }
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics, new TimeWaitConsumerLogRebalanceListener(this));
        consume();
    }

    private class NamingThreadFactory implements ThreadFactory {
        private final AtomicLong              count = new AtomicLong(0l);
        private KafkaConsumer<String, String> consumer;

        public NamingThreadFactory(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;

        }

        @Override
        public Thread newThread(Runnable r) {
            if (name.isEmpty()) {
                regenerateName();
            }
            return new Thread(r, name + "-" + count.incrementAndGet());
        }
    }

    private class CustomCallerRunsPolicy extends ThreadPoolExecutor.CallerRunsPolicy {
        private AtomicLong                    count = new AtomicLong(0l);
        private KafkaConsumer<String, String> consumer;

        public CustomCallerRunsPolicy(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            if (name.isEmpty()) {
                regenerateName();
            }
            LOG.info("Rejection policy invoked ..." + name + " total rejected count {}", count.incrementAndGet());
            super.rejectedExecution(r, e);
        }
    }

}
