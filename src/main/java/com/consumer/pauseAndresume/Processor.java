package com.consumer.pauseAndresume;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class Processor extends GenericProcessor{

    @Override
    public Map<String, Future<Object>> process(ExecutorService service, Map<String, LinkedList<String>> batchRequest) {
        //TODO: consume message
        return Collections.emptyMap();
    }

}
