package com.sbsa.benchmark.solaceclient;

import java.util.concurrent.ConcurrentHashMap;

import com.sbsa.benchmark.Consumer;
import com.sbsa.benchmark.DeliveryMode;
import com.sbsa.benchmark.EndPointType;
import com.sbsa.benchmark.Endpoint;
import com.sbsa.benchmark.Factory;
import com.sbsa.benchmark.Monitor;
import com.sbsa.benchmark.Producer;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;

public class SolClientFactory extends Factory {

	JCSMPFactory jcsmp = JCSMPFactory.onlyInstance();
	final JCSMPProperties properties = new JCSMPProperties();

	public SolClientFactory(ConcurrentHashMap<String, Object> runtimeParamaters) throws Exception {
		super(runtimeParamaters);
		initialize();
	}

	private void initialize() throws Exception {
		properties.setProperty(JCSMPProperties.HOST, (String) runtimeParamaters.get("BrokerIP"));
		properties.setProperty(JCSMPProperties.VPN_NAME, "default");
		properties.setProperty(JCSMPProperties.USERNAME, "clientUsername");
	}

	private JCSMPSession createSession() throws JCSMPException {
		final JCSMPSession session = jcsmp.createSession(properties);
		session.connect();
		return session;
	}

	@Override
	public Producer createProducer(Monitor monitor, Endpoint endpoint) throws Exception {
		Producer producer = new SolClientProducer(monitor, endpoint);
		return producer;
	}

	@Override
	public Consumer createConsumer(Monitor monitor, Endpoint endpoint) throws Exception {
		Consumer consumer = new SolClientConsumer(monitor, endpoint);
		return consumer;
	}

	@Override
	public Endpoint createEndPoint(String destination) throws Exception {
		Endpoint endpoint = new Endpoint();
		EndPointType endPointType = (EndPointType) runtimeParamaters.get("EndpointType");
		DeliveryMode deliveryMode = (DeliveryMode) runtimeParamaters.get("DeliveryMode");
		JCSMPSession session = createSession();

		switch (endPointType) {
		case Topic: {
			endpoint.setEndpoint(endPointType, deliveryMode, null, session, jcsmp.createTopic(destination));
		}
			break;
		case Queue: {
			final Queue queue = jcsmp.createQueue(destination);
			final EndpointProperties endpointProps = new EndpointProperties();
			endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
			endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
			session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
			endpoint.setEndpoint(endPointType, deliveryMode, null, session, queue);
		}
			break;
		default:
			throw new RuntimeException("No Endpoint defined");
		}
		return endpoint;
	}

}
