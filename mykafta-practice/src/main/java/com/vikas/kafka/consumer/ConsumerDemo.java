package com.vikas.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vikas.kafka.util.Util;

public class ConsumerDemo {

	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);

	public static void main(String[] args) {
		String groupId = "my-fourth-application";
		String topic = "first_topic";

		// Initialize properties
		Properties properties = Util.initializeConsumerProperties(groupId);

		// Create a consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		// Subscribe to topic or topics
		consumer.subscribe(Arrays.asList(topic));

		// Poll for new data
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				LOGGER.info(String.format("Key %s", record.key()));
				LOGGER.info(String.format("Value %s", record.value()));
				LOGGER.info(String.format("Offset %s", record.offset()));
				LOGGER.info(String.format("Partition %s", record.partition()));
			}
		}
	}
}
