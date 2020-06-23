package com.learning.kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallBack {

	//public Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

	public static void main(String[] args) {
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
		System.out.println("hello kafka bis");
		
		//create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); 
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create the Producer 
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		for (int i = 0; i < 10; i++) {
			
		
		//ceate a producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello kafka again"+Integer.toString(i));
		
		//send data
		producer.send(record, new Callback(){
		
			public void onCompletion(RecordMetadata metadata, Exception e) {
				// executes every time a record is successfully sent or an exception is thrown
				if (e == null) {
					//the record is successfully sent
					logger.info("###########################Receiver new metadata");
					logger.info("Topic [{}]", metadata.topic());
					logger.info("Partition [{}]", metadata.partition());
					logger.info("Offset [{}]", metadata.offset());
					logger.info("Timestamp [{}]", metadata.timestamp());

				}else {
					logger.error("Error while producing", e);
				}
			}
		});
	}
		//flush data
		producer.flush();
		//flush and close data
		producer.close();
		
	}

}
