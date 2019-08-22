package com.consumer.pauseresume;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.consumer.pauseAndresume.ConsumersStart;

public class ConsumersStartIT {

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void start() throws InterruptedException {
        ConsumersStart listenerThread = new ConsumersStart();
        listenerThread.init();

        Thread.sleep(300 * 60 * 1000);
    }

}
