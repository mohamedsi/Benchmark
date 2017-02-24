package com.sbsa.benchmark.natsio;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

//import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import com.sbsa.benchmark.Consumer;
import com.sbsa.benchmark.DeliveryMode;
import com.sbsa.benchmark.EndPointType;
import com.sbsa.benchmark.Endpoint;
import com.sbsa.benchmark.Factory;
import com.sbsa.benchmark.Monitor;
import com.sbsa.benchmark.Producer;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;

public class NatsIOFactory extends Factory{
	
	ConnectionFactory nc;
	
	public NatsIOFactory(ConcurrentHashMap<String, Object> runtimeParamaters) throws Exception {
		super(runtimeParamaters);
		initialize();
	}
	
	private void initialize() throws Exception {
		nc =  new ConnectionFactory();
	}
	
	private Object createConnection() throws IOException {
		switch ((EndPointType) runtimeParamaters.get("EndpointType")) {
		case Queue: {
			return nc.createConnection();
		}
		case Topic: {
			return nc.createConnection();
		}
		default:
			throw new RuntimeException("No Endpoint defines");
		}
	}

	@Override
	public Producer createProducer(Monitor monitor, Endpoint endpoint) throws Exception {
		Producer producer = new NatsIOProducer(monitor, endpoint);
		return producer;
	}

	@Override
	public Consumer createConsumer(Monitor monitor, Endpoint endpoint) throws Exception {
		Consumer consumer = new NatsIOConsumer(monitor, endpoint);
		return consumer;
	}

	@Override
	public Endpoint createEndPoint(String destination) throws Exception {
		Endpoint endpoint = new Endpoint();
		EndPointType endPointType = (EndPointType) runtimeParamaters.get("EndpointType");
		DeliveryMode deliveryMode = (DeliveryMode) runtimeParamaters.get("DeliveryMode");
		Connection connection = (Connection) createConnection();
				
		switch (endPointType) {
		case Queue: {
			endpoint.setEndpoint(endPointType, deliveryMode, connection, null, destination);
		}
			break;
		case Topic: {
			endpoint.setEndpoint(endPointType, deliveryMode, connection, null, destination);
		}
			break;
		default:
			throw new RuntimeException("No Endpoint defined");
		}
		return endpoint;
	}
	
	
}