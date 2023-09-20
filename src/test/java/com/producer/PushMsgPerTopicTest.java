package com.producer;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PushMsgPerTopicTest {
    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    // send message to particular topic without knowing the partition
    @Test
    public void testSend() {
        String topic = "dataitems";
        List<String> data = new ArrayList<>();

        long delta = System.currentTimeMillis() + (2 * 60 * 1000);
        data.add(delta + "=message" + delta);
        //data.add("i am johan ashdjjl kkl");
        //data.add("you there ? nhkahk");
        //data.add("need to talk");
        //data.add("ok bye");
        //data.add("im fine");
        //data.add("you ok !!!");
        //data.add("im on leave");
        //data.add("not coming to office");
        PushMsgPerTopic.send(topic, data);
        PushMsgPerTopic.close();
    }

    // send message to all partitions created in kafka for topic
    //@Test
    public void testSendRoundRobin() {
        String topic = "dataitems";
        int totalPartition = 4;
        List<String> data = new ArrayList<>();
        data.add("hello msg");
        data.add("i am johan");
        data.add("you there ?");
        data.add("i am waiting !!");
        data.add("i will be leaving now");
        data.add("bye bye");
        data.add("see you later");
        PushMsgPerTopic.sendAcrossPartitions(topic, totalPartition, data);
        PushMsgPerTopic.close();
    }

}
