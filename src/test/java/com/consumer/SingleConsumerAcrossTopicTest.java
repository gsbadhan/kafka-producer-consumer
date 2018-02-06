package com.consumer;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SingleConsumerAcrossTopicTest {
	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	// consume messages from given topics
	@Test
	public void testConsumeForGivenTopic() {
		List<String> topics = new ArrayList<>();
		topics.add("dataitems");
		topics.add("events");
		SingleConsumerAcrossTopic.consume(topics);
		SingleConsumerAcrossTopic.close();
	}

	// consume messages from all topics
	@Test
	public void testConsumeFromAllTopics() {
		SingleConsumerAcrossTopic.consume();
		SingleConsumerAcrossTopic.close();
	}
}
