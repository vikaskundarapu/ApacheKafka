package com.vikas.kafka.consumer.elasticsearch;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;
import com.vikas.kafka.consumer.elasticsearch.util.Util;

/*
 * One generic way. This will always be unique String id = record.topic() + "_"
 * + record.partition() + "_" + record.offset();
 */
public class ElasticSearchConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

	public static RestHighLevelClient createElasticSearchClient(String bonsaiUri) {

		URI connUri = URI.create(bonsaiUri);
		String[] auth = connUri.getUserInfo().split(":");

		CredentialsProvider cp = new BasicCredentialsProvider();
		cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

		RestHighLevelClient rhlc = new RestHighLevelClient(
				RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
						.setHttpClientConfigCallback(
								httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)));
		return rhlc;
	}

	public static KafkaConsumer<String, String> createKafkaConsumer(String groupId, String topic) {
		Properties properties = Util.initializeConsumerProperties(groupId);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}

	public static void main(String[] args) throws IOException {
		String bonsaiUri = "BONSAI-URI";
		RestHighLevelClient searchClient = createElasticSearchClient(bonsaiUri);

		String groupId = "kafka-demo-elasticsearch";
		String topic = "twitter_tweets";
		KafkaConsumer<String, String> consumer = createKafkaConsumer(groupId, topic);

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			int recordCount = records.count();
			LOGGER.info("Received: " + recordCount + " records");

			BulkRequest bulkRequest = new BulkRequest();
			for (ConsumerRecord<String, String> record : records) {
				/*
				 * We will twitter id_str which is a unique id to make this consumer idempotent
				 */
				String value = record.value();
				String id = extractIdFromTweet(value);

				IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(value, XContentType.JSON);
				bulkRequest.add(indexRequest);
			}

			if (recordCount > 0) {
				BulkResponse bulkResponse = searchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
				LOGGER.info("Committing offsets!");
				consumer.commitSync();
				LOGGER.info("Offsets have been committed!");

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		// searchClient.close();
	}

	private static String extractIdFromTweet(String tweet) {
		return JsonParser.parseString(tweet).getAsJsonObject().get("id_str").getAsString();
	}
}
