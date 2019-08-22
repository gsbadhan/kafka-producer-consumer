package com.consumer.pauseAndresume;

import java.rmi.server.UID;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.slf4j.MDC;



public abstract class GenericProcessor {

    public abstract Map<String, Future<Object>> process(ExecutorService service, Map<String, LinkedList<String>> batchRequest);

    public static void generateRequestId() {
        UID unique = new UID();
        MDC.put("requestId", "c-" + unique.toString());
    }

    public static void generateRequestId(String prefix) {
        UID unique = new UID();
        MDC.put("requestId", prefix + "-" + unique.toString());
    }
}
