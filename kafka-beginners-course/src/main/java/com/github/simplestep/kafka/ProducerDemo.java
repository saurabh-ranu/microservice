package com.github.simplestep.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
		String bootstrapServer = "127.0.0.1:9092";
		
		// create producer properties
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer(properties);
		
		// create a producer record
		ProducerRecord<String, String> record = new ProducerRecord("first_topic", "hello world");
		
		// send data - asynchronously
		producer.send(record);
		
		// flush data
		producer.flush();
		
		// flush and close data
		producer.close();
	}

}
