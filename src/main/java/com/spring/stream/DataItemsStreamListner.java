package com.spring.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DataItemsStreamListner {
    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        KStream<String, String> kStream = streamsBuilder.stream("dataitems", Consumed.with(STRING_SERDE, STRING_SERDE));
        log.info("dataitems stream ={}", kStream);
        kStream.foreach(new ForeachAction<String, String>() {
            @Override
            public void apply(String key, String value) {
                log.info("dataitems stream key={}, value={}", key, value);

                //TODO: event processing logic

            }
        });
    }
}
