package com.sbsa.benchmark.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.sbsa.benchmark.Consumer;
import com.sbsa.benchmark.Endpoint;
import com.sbsa.benchmark.Monitor;

public class KafkaClientConsumer extends Consumer {

	private KafkaConsumer consumer;
	private String endPoint;

	public KafkaClientConsumer(Monitor monitor, Endpoint endpoint) throws Exception {
		super(monitor, endpoint);
		initialise();
	}

	private void initialise() throws Exception {
		endPoint = (String) this.endpoint.getEndpoint();

		Properties properties = new Properties();
		properties.put("bootstrap.servers", (String) monitor.runtimeParamaters.get("BrokerIP") + ":"
				+ (String) monitor.runtimeParamaters.get("BrokerPort"));
		properties.put("group.id", "group");
		properties.put("session.timeout.ms", "200");
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("heartbeat.interval.ms", "100");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		properties.put("group.max.session.timeout.ms", "1000");
		properties.put("group.min.session.timeout.ms", "100");
		
		consumer = new KafkaConsumer(properties);
		consumer.subscribe(Arrays.asList(endPoint.replace("/", "")));
	}

	@Override
	public void start() throws Exception {
		this.monitor.executor.execute(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						ConsumerRecords<String, String> records = consumer.poll(10);
						long recievedTime = System.nanoTime();
						for (ConsumerRecord<String, String> record : records) {
							process(record.timestamp());
						}
					} catch (Exception e) {
						System.out.println(e.getMessage());
					}
				}
			}
		});
	}

}
