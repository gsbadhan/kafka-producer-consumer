package com.stream;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class KeywordCountStream {
	private KeywordCountStream() {
	}

	private static Properties config = new Properties();

	static {
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "keywordCount-app");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	}

	public static void startKeywordCountStream() {
		final String topic = "keywords";
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		KStream<String, String> kStream = streamsBuilder.stream(topic);
		KTable<String, Long> keywordCounts = kStream
				.flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
				.groupBy((key, word) -> word)
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("keyword-count-store"));

		keywordCounts.toStream().to("keywords-count", Produced.with(Serdes.String(), Serdes.Long()));
		KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), config);
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		System.out.println("keywords topic stteaming started..");
	}

	public static void startUserClickMonitoringStream() {
		final String topic = "userclicks";
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		KStream<String, String> kStream = streamsBuilder.stream(topic);
		KTable<String, Long> clickCount = kStream.flatMapValues(data -> Arrays.asList(data.replaceFirst("\\|", "-")))
				.groupBy((k, v) -> v)
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("userclickcount"));
		clickCount.toStream().to("userclickcount", Produced.with(Serdes.String(), Serdes.Long()));
		KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), config);
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		System.out.println("user click count started..");
	}

}
