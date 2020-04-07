package com.vikas.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vikas.kafka.util.Util;

/**
 * Used to read some records
 */
public class ConsumerDemoAssignSeek {

	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

	public static void main(String[] args) {
		String groupId = "my-fourth-application";
		String topic = "first_topic";

		// Initialize properties
		Properties properties = Util.initializeConsumerProperties(groupId);

		// Create a consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		TopicPartition partition = new TopicPartition(topic, 0);
		consumer.assign(Arrays.asList(partition));

		long offset = 5L;
		consumer.seek(partition, offset);

		int maxOffsets = 5;
		boolean isMaxOffset = true;
		int offsetsRead = 0;
		// Poll for new data
		while (isMaxOffset) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				offsetsRead++;
				LOGGER.info(String.format("Key %s", record.key()));
				LOGGER.info(String.format("Value %s", record.value()));
				LOGGER.info(String.format("Offset %s", record.offset()));
				LOGGER.info(String.format("Partition %s", record.partition()));

				if (offsetsRead > maxOffsets) {
					isMaxOffset = false;
					break;
				}

			}

		}
		LOGGER.info(String.format("Exiting the application"));

	}
}
