package com.vikas.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vikas.kafka.util.Util;

public class ConsumerRunnable implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRunnable.class);
	private CountDownLatch latch;
	private KafkaConsumer<String, String> consumer;

	ConsumerRunnable(CountDownLatch latch, String groupId, String topic) {
		this.consumer = new KafkaConsumer<>(Util.initializeConsumerProperties(groupId));
		consumer.subscribe(Arrays.asList(topic));
		this.latch = latch;
	}

	@Override
	public void run() {

		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					LOGGER.info(String.format("Key %s", record.key()));
					LOGGER.info(String.format("Value %s", record.value()));
					LOGGER.info(String.format("Offset %s", record.offset()));
					LOGGER.info(String.format("Partition %s", record.partition()));
				}
			}
		} catch (WakeupException e) {
			LOGGER.info("WakeupException occured i.e. consumer is interrupted. Received Shutdown signal!");
		} finally {
			consumer.close();
			latch.countDown();
		}

	}

	public void shutdown() {
		/*
		 * used to interrupt consumer.poll(), throws WakeUpException
		 */
		consumer.wakeup();
	}

}
