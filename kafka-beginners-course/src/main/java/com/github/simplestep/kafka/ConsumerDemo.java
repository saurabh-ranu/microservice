package com.github.simplestep.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
	
	public static void main(String[] args) {
		String bootstrapServer = "127.0.0.1:9092";
		String group_id = "my_fifth_application";
		String topic = "first_topic";

		// create consumer properties
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, group_id);
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		// create the consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);

		// subscribe consumer to our topic(s)
		consumer.subscribe(Arrays.asList(topic));
		
		//poll for new data
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord record: records) {
				logger.info("Key: "+record.key() + " Value: "+record.value());
				logger.info("Partition: "+record.partition() + " Offset: "+record.offset());
				
			}
		}

	}

}
