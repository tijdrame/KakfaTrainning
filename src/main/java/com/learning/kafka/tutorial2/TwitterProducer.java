package com.learning.kafka.tutorial2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TwitterProducer
 * create topic twitter_tweets
 * lancer un consumer sur ce topic: kafka-console-consumer.bat --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets
 * puis lancer cette classe
 */
public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	private String consumerKey = "OnStaYkIgHyzX94zXnOaxjfxQ";
	private String consumerSecret = "MycrJPJLneH2dYycRzXWvnCdGrRt3KTa1cDbtb636NJd7zXAHl";
	private String token = "280689699-CtBjVmXRYqfHhgR0MmfBFA2gFmJnmU7fe55utDMw";
	private String secret = "3ISLWVSetCbShYNbPuBjVDqLzx5TLHTID9gsTzY74aCSU";
	private List<String> terms = Lists.newArrayList("kafka", "java", "covid");

	public TwitterProducer() {
	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

	public void run() {
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		// create a twitter client
		Client client = createTwitterClient(msgQueue);
		// Attempts to establish a connection.
		client.connect();
		// create a kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		// add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Stopping application");
			logger.info("Stopping client from twitter....");
			client.stop();
			logger.info("Closing  produer ....");
			producer.close();
			logger.info("Done!!!");
		}));
		
		// loop to send tweets to kafka
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {
				logger.info("msg == [{}]", msg);
				// creer twitter_tweets avec la cmd kafka-topics --zookeeper localhost:2181 --create --topic	
				producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if (e != null) {
							logger.error("Something bad happened", e);
						}
						
					}
				});
			}
		}
		logger.info("End of app");
	}

	private KafkaProducer<String, String> createKafkaProducer() {
		// create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		//add prop for safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");//kafka 2.0 >=1.1 so we can keep this as 5. use 1 otherwise

		// augementer performance producer avec compresion, batch..
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");// snappy by google
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");//definit l'attente avt d'envoyer le msg
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString((32*1024)));//32 KB batch size

		// create the Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		return producer;
	}

	// https://github.com/twitter/hbc
	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);
		
		// hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		// .eventMessageQueue(eventQueue); // optional: use this
		// if you want to
		// process client
		// events

		Client hosebirdClient = builder.build();

		return hosebirdClient;
	}

}