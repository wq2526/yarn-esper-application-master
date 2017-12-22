package com.kafka.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerClient<K, V> {
	
	private static final Log LOG = LogFactory.getLog(KafkaProducerClient.class);
	
	private Producer<K, V> producer;
	private List<String> topics;
	private Properties props;
	
	private boolean running;
	
	public KafkaProducerClient(String server){
		
		props = new Properties();
		props.put("bootstrap.servers", server);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", 
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", 
				"org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<K, V>(props);
		
		LOG.info("set up kafka producer with server " + server);
		topics = new ArrayList<String>();
		
		running = true;
		
	}
	
	public void produce(K key, V value) {
		
		for(String topic : topics){
			//LOG.info("send message " + value +" to topic " + topic);
			producer.send(new ProducerRecord<K, V>(topic, key, value));
		}	
		
	}
	
	public void addTopic(String topic) {
		LOG.info("add topic " + topic + " for kafka producer");
		topics.add(topic);
	}
	
	public boolean getRunning() {
		return running;
	}
	
	public void setRunning(boolean running) {
		this.running = running;
	}
	
	public void close() {
		LOG.info("close kafka producer");
		producer.close();
	}

}
