package com.github.simplestep.kafka.twitterintegration;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
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

public class TwitterProducer {

	static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
	
	String consumerKey = "GBxVFa2nh4nKd3g4w7H0tS221";
	String consumerSecret = "MawinX1IgBzxH5HhdnkBTM8296ZWiQOMeBDmPhmnsUJa2Sva5A";
	String token = "130538125-JMvD3T8Lf33Z0uh0c9pU7F0X0SkXjFzmUUz9xPTF";
	String secret = "9PVvC8TAYRjhSGtVKZofkPYxID2jro3ibba7Utp7Fthlf";
	List<String> terms = Lists.newArrayList("kafka");
	
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
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		
		// create a twitter client
		Client client = createTwitterClient(msgQueue);
		// Attempts to establish a connection.
		client.connect();
		
		// create a kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		Runtime.getRuntime().addShutdownHook(new Thread(()-> {
			logger.info("stopping application.");
			logger.info("shutting down client from twitter.");
			client.stop();
			logger.info("closing producer.");
			producer.close();
			logger.info("done!");
		}));
		
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
		  try {
			msg = msgQueue.poll(5,TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
			client.stop();
		}
		  if(msg!=null) {
			  logger.info(msg);
			  producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e!=null) {
						logger.error("Something bad happened.",e);
					}
					
				}
			});
		  }
		  
		}
		
		
		logger.info("Ends of application");
		
		// loop to send tweets to kafka
		
	}



	private KafkaProducer<String, String> createKafkaProducer() {
		String bootstrapServer = "127.0.0.1:9092";
		
		// create producer properties
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer(properties);
		return producer;
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue)); // optional: use this if you want to process client
																	// events

		Client hosebirdClient = builder.build();
		return hosebirdClient;

	}

}
