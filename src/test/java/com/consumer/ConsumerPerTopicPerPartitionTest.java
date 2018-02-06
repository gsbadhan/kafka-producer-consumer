package com.consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConsumerPerTopicPerPartitionTest {

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	// consume messages from given topics
	@Test
	public void testConsumeForGivenTopic() {
		String topic = "dataitems";
		int partition = 0;
		ConsumerPerTopicPerPartition.consume(topic, partition);
		ConsumerPerTopicPerPartition.close();
	}

}
