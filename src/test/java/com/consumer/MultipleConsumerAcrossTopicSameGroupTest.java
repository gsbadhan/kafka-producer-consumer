package com.consumer;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MultipleConsumerAcrossTopicSameGroupTest {
    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    // consume messages from all topics
    @Test
    public void testConsumeFromAllTopics() throws InterruptedException {
        List<String> topics = new ArrayList<>();
        topics.add("testkeyhash");
        SingleConsumerAcrossTopic2 acrossTopic1 = new SingleConsumerAcrossTopic2();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    acrossTopic1.consume(topics);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        //
        SingleConsumerAcrossTopic2 acrossTopic2 = new SingleConsumerAcrossTopic2();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    acrossTopic2.consume(topics);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        Thread.sleep(10 * 60 * 1000);
        acrossTopic1.close();
        acrossTopic2.close();
    }
}
