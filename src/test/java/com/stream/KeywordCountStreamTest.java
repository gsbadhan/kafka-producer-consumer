package com.stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KeywordCountStreamTest {
	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	@Test
	public void testStartKeywordCountStream() throws InterruptedException {
		KeywordCountStream.startKeywordCountStream();
		// wait for
		Thread.sleep(30 * 60 * 1000);
	}

	@Test
	public void testStartUserClickMonitoringStream() throws InterruptedException {
		KeywordCountStream.startUserClickMonitoringStream();
		// wait for
		Thread.sleep(30 * 60 * 1000);
	}

}
