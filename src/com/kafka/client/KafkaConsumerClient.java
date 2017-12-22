package com.kafka.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerClient<K, V> {
	
	private static final Log LOG = LogFactory.getLog(KafkaConsumerClient.class);
	
	private Consumer<K, V> consumer;
	private List<String> topics;
	private Properties props;
	
	private boolean running;
	
	public KafkaConsumerClient(String server, String groupId) {
		props = new Properties();
		props.put("bootstrap.servers", server);
	    props.put("group.id", groupId);
	    props.put("enable.auto.commit", "true");
	    props.put("auto.commit.interval.ms", "1000");
	    props.put("auto.offset.reset", "earliest");
	    props.put("key.deserializer", 
	    		"org.apache.kafka.common.serialization.StringDeserializer");
	    props.put("value.deserializer", 
	    		"org.apache.kafka.common.serialization.StringDeserializer");
		
		consumer = new KafkaConsumer<K, V>(props);
		
		LOG.info("set up kafka consumer with server " + server);
		topics = new ArrayList<String>();
		
		running = true;
		
	}
	
	public void subscibe() {
		consumer.subscribe(topics);
	}
	
	public ConsumerRecords<K, V> consume() {
	    
		LOG.info("kafka consumer start to consume");
		ConsumerRecords<K, V> records = consumer.poll(100);
		return records;
		
	}
	
	public void addTopic(String topic) {
		LOG.info("add topic " + topic + " for kafka consumer");
		topics.add(topic);
	}
	
	public boolean getRunning() {
		return running;
	}
	
	public void setRunning(boolean running) {
		this.running = running;
	}
	
	public void close() {
		LOG.info("close kafka consumer");
		consumer.close();
	}

}
