package com.vikas.kafka.producer;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vikas.kafka.util.Constants;
import com.vikas.kafka.util.Util;

public class ProducerDemoUsingKeys {

	private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoUsingKeys.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		Properties producerProperties = Util.initializeProducerProperties();
		KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
		String topic = "first_topic";

		for (int i = 0; i < 10; i++) {

			String key = "id_" + i;
			String value = "Hello Agent " + i + "! " + Util.getAdvice(i);
			LOGGER.info(String.format("Key: %s", key));// Logger not needed. Included just for our understanding
			ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
			producer.send(record, new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {

					if (Objects.isNull(exception)) {
						LOGGER.info(String.format("Topic : %s ", metadata.topic()));
						LOGGER.info(String.format("Offset : %s ", metadata.offset()));
						LOGGER.info(String.format("Partition : %s ", metadata.partition()));
						LOGGER.info(String.format("Timestamp : %s ", metadata.timestamp()));
					} else {
						LOGGER.error("Error occcurred while producing results: ", exception);
					}
				}
			}).get();// NOTE: We should not call get() method as this enables synchronous execution
						// and blocks.
		}

		/*
		 * Flush and close are important else the consumers wont be able to find the
		 * data sent
		 */
		producer.flush();
		producer.close();

	}

}
