package com.vikas.kafka.twitter;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.vikas.kafka.twitter.util.Util;

public class TwitterProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class);
	private static final String CONSUMER_KEY = "CONSUMER_KEY";
	private static final String CONSUMER_SECRET = "CONSUMER_SECRET";
	private static final String TOKEN = "TOKEN";
	private static final String SECRET = "SECRET";

	/**
	 * Set up your blocking queues: Be sure to size these properly based on expected
	 * TPS of your stream
	 */
	BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

	public static void main(String[] args) {
		new TwitterProducer().produceTweets();
	}

	public void produceTweets() {

		LOGGER.info("Application starts!");

		// Create a Twitter client
		Client client = initializeTwitterClient(msgQueue);
		client.connect();

		// Create a Kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();

		// Process the tweets
		processTweets(producer, client);
		addShutDownHook(producer, client);

		LOGGER.info("Application processing is completed!");
	}

	private void addShutDownHook(KafkaProducer<String, String> producer, Client client) {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			LOGGER.info("Stopping the twitter client");
			LOGGER.info("Closing the producer");
			client.stop();
			producer.close();
			LOGGER.info("Application is shutdown");
		}));

	}

	private void processTweets(KafkaProducer<String, String> producer, Client client) {
		while (!client.isDone()) {
			try {
				String message = msgQueue.poll(5, TimeUnit.SECONDS);
				String topic = "twitter_tweets";
				if (Objects.nonNull(message)) {
					LOGGER.info(message);

					ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, message);
					producer.send(record, new Callback() {
						@Override
						public void onCompletion(RecordMetadata metadata, Exception exception) {
							if (Objects.nonNull(exception))
								LOGGER.error("Error occurred!", exception);
						}
					});
				}
			} catch (InterruptedException e) {
				client.stop();
				e.printStackTrace();
			}
		}
		client.stop();
	}

	private KafkaProducer<String, String> createKafkaProducer() {
		Properties properties = Util.initializeProducerProperties();
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		return producer;
	}

	private Client initializeTwitterClient(BlockingQueue<String> msgQueue) {
		LOGGER.info("Initializing the Twitter client");

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("poems");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		LOGGER.info("Twitter client initialized");
		return hosebirdClient;
	}
}
