package com.producer;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PushMsgPerPartitionTest {
	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	// send message to particular partition
	@Test
	public void testSend() {
		String topic = "dataitems";
		int partition = 0;
		List<String> data = new ArrayList<>();
		data.add("hello msg");
		data.add("i am johan");
		data.add("you there ?");
		PushMsgPerPartition.send(topic, partition, data);
		PushMsgPerPartition.close();
	}

	// send message to all partitions created in kafka for topic
	@Test
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
		PushMsgPerPartition.sendAcrossPartitions(topic, totalPartition, data);
		PushMsgPerPartition.close();
	}

}
