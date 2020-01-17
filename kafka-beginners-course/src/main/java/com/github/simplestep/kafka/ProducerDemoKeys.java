package com.github.simplestep.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {

	static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		String bootstrapServer = "127.0.0.1:9092";
		
		// create producer properties
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer(properties);
		for(int i=0; i<10;i++) {
			String topic = "first_topic";
			String value = "hello world " +i;
			String key = "id_"+Integer.toString(i);
		// create a producer record
		ProducerRecord<String, String> record = new ProducerRecord(topic, key, value);
		
		logger.info("Key: "+key);
		
		// Same key is going to same partition every time
		// id_0 Partition: 1
		// id_1 Partition: 0
		// id_2 Partition: 2
		// id_3 Partition: 0
		// id_4 Partition: 2
		// id_5 Partition: 2
		// id_6 Partition: 0
		// id_7 Partition: 2
		// id_8 Partition: 1
		// id_9 Partition: 2
		
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
		}).get(); // block the .send() to make it synchronous - don't do it in production 
		}
		// flush data
		producer.flush();
		
		// flush and close data
		producer.close();
	}

}
