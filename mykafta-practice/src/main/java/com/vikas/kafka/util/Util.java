package com.vikas.kafka.util;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Util {

	private static final Properties properties = new Properties();

	private Util() {
	}

	public static Properties initializeProducerProperties() {
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}

	public static String getAdvice(int i) {
		final String[] advises = new String[] { "Operation Zulfikaar is started!",
				"Find Manoj BachPai and inform him about Zulfikaar!", "Musa needs proper treatment!",
				"Find out if Musa is innocent?", "Find Musa's mother!", "Find out who is funding all this?",
				"We need to get our sources working!", "Find the man soon and fix him good!", "Need to find X soon!",
				"Call all our resources and find more information!" };
		return advises[i];
	}
	
	public static Properties initializeConsumerProperties(String groupId) {
		
		
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ,StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ,StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return properties;
	}
}
