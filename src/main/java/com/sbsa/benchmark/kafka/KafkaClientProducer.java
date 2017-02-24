package com.sbsa.benchmark.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.sbsa.benchmark.Endpoint;
import com.sbsa.benchmark.Monitor;


public class KafkaClientProducer extends com.sbsa.benchmark.Producer {
    private static org.apache.kafka.clients.producer.Producer<Integer, String> producer;
	private final Properties properties = new Properties();

	public KafkaClientProducer(Monitor monitor, Endpoint endpoint) throws Exception {
		super(monitor, endpoint);
		initialise();
	}

	private void initialise() {
		properties.put("bootstrap.servers", (String) monitor.runtimeParamaters.get("BrokerIP")+":"+(String) monitor.runtimeParamaters.get("BrokerPort"));
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer(properties);
	}

	@Override
	public void publish(String message) throws Exception {
		final long startTime;
		final long endTime;

		startTime = System.nanoTime();
		producer.send(new ProducerRecord(((String) endpoint.getEndpoint()).replace("/", ""), null, startTime, "message", message));
		endTime = System.nanoTime();

		this.recordStats(startTime, endTime);
	}

}
