package com.vikas.kafka.consumer;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Run this class 3 times one after another and check how re-organization of partitions happens
 * */
public class ConsumerDemoWithThread {

	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

	public static void main(String[] args){
		String groupId = "my-sixthapplication";
		String topic = "first_topic";

		CountDownLatch latch = new CountDownLatch(1);
		Runnable myConsumerRunnable = new ConsumerRunnable(latch, groupId, topic);
		Thread consumerThread = new Thread(myConsumerRunnable);
		consumerThread.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			LOGGER.info("Application is consumed completely!");
			((ConsumerRunnable) myConsumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();	
			}
			LOGGER.info("Application has exited!");
		}));
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			LOGGER.error("Application got interrupt!");
		}finally {
			LOGGER.error("Application is closing!");
		}
	}
}
