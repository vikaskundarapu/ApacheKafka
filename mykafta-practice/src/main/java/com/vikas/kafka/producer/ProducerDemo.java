package com.vikas.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.vikas.kafka.util.Constants;
import com.vikas.kafka.util.Util;

public class ProducerDemo {

	public static void main(String[] args) {

		/*
		 * Recollect that while running through cmd we can use something like:
		 * kafka-console-producer.bat --broker-list 127.0.0.1:9092 --topic new_topic_2
		 * --producer-property acks=all. So, we need to specify the properties and
		 * topics.
		 */

		/* Step 1: Set producer properties */
		Properties producerProperties = Util.initializeProducerProperties();

		/*
		 * Step 2: Create a Kafka producer. The data sent by producer should be
		 * represented as String
		 */
		KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
		String topic = "first_topic";
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Hello! Consume the data from Java!");

		/* Step 3: Send the data */
		producer.send(record);

		/*
		 * Flush and close are important else the consumers wont be able to find the
		 * data sent
		 */
		producer.flush();
		producer.close();

	}

}
