package com.consumer.pauseAndresume;


public interface ConsumerListenerThreads {

    void init();

    int getConsumerPool();

    void setConsumerPool(int consumerPool);

    int getThreadPoolPerConsumer();

    void setThreadPoolPerConsumer(int threadPoolPerConsumer);

    void destroyNow();

    void startConsumers();
    
    void pauseAllConsumers();
    
    void resumeAllConsumers();

    void preDestroy();

}