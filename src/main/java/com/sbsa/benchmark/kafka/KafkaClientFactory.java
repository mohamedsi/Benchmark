package com.sbsa.benchmark.kafka;

import java.util.concurrent.ConcurrentHashMap;

import com.sbsa.benchmark.Consumer;
import com.sbsa.benchmark.DeliveryMode;
import com.sbsa.benchmark.EndPointType;
import com.sbsa.benchmark.Endpoint;
import com.sbsa.benchmark.Factory;
import com.sbsa.benchmark.Monitor;
import com.sbsa.benchmark.Producer;

public class KafkaClientFactory extends Factory {

	public KafkaClientFactory(ConcurrentHashMap<String, Object> runtimeParamaters) throws Exception {
		super(runtimeParamaters);
		initialize();
	}

	private void initialize() throws Exception {

	}


	@Override
	public Producer createProducer(Monitor monitor, Endpoint endpoint) throws Exception {
		Producer producer = new KafkaClientProducer(monitor, endpoint);
		return producer;
	}

	@Override
	public Consumer createConsumer(Monitor monitor, Endpoint endpoint) throws Exception {
		Consumer consumer = new KafkaClientConsumer(monitor, endpoint);
		return consumer;
	}

	@Override
	public Endpoint createEndPoint(String destination) throws Exception {
		EndPointType endPointType = (EndPointType) runtimeParamaters.get("EndpointType");
		DeliveryMode deliveryMode = (DeliveryMode) runtimeParamaters.get("DeliveryMode");
		Endpoint endpoint = new Endpoint();
		endpoint.setEndpoint(endPointType, deliveryMode, null, null, destination);
		return endpoint;
	}

}
