package com.vikas.kafka.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class KafkaStreamsFilterTweets {

	public static void main(String[] args) {

		// create properties
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());

		// create a topology
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		// input topic
		KStream<String, String> stream = streamsBuilder.stream("twitter_tweets");

		KStream<String, String> filteredStream = stream
				.filter((K, jsonTweet) -> extractUserOnFollowerCount(jsonTweet, 100000));

		String topic = "important_tweets";
		filteredStream.to(topic);

		// build the topology
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

		// start our streams application
		kafkaStreams.start();
	}

	private static boolean extractUserOnFollowerCount(String tweet, int minimumFollowers) {
		return JsonParser.parseString(tweet).getAsJsonObject().get("user").getAsJsonObject().get("followers_count")
				.getAsInt() > minimumFollowers;
	}
}
