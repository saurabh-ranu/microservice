package com.github.simplestep.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

	static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
	
	public static void main(String[] args) {
		String bootstrapServer = "127.0.0.1:9092";
		
		// create producer properties
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer(properties);
		for(int i=0; i<10;i++) {
		// create a producer record
		ProducerRecord<String, String> record = new ProducerRecord("first_topic", "hello world " +i);
		
		// send data - asynchronously
		producer.send(record, new Callback() {
			
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// TODO Auto-generated method stub
				if(exception==null) {
					// the record was successfully sent
					logger.info(" Received new metadata. \n"+
					"Topic: "+metadata.topic() + "\n" +
					"Partition: "+metadata.partition() + "\n" +
					"Offset: "+metadata.offset() + "\n" +
					"Timestamp: "+metadata.timestamp());
				}else {
					logger.info("Error while producing",exception);
				}
				
			}
		});
		}
		// flush data
		producer.flush();
		
		// flush and close data
		producer.close();
	}

}
