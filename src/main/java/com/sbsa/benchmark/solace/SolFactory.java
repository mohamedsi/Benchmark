package com.sbsa.benchmark.solace;

import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.Session;

import com.sbsa.benchmark.Consumer;
import com.sbsa.benchmark.DeliveryMode;
import com.sbsa.benchmark.EndPointType;
import com.sbsa.benchmark.Endpoint;
import com.sbsa.benchmark.Factory;
import com.sbsa.benchmark.Monitor;
import com.sbsa.benchmark.Producer;
import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import com.solacesystems.jms.SupportedProperty;

public class SolFactory extends Factory {

	SolConnectionFactory cf;

	public SolFactory(ConcurrentHashMap<String, Object> runtimeParamaters) throws Exception {
		super(runtimeParamaters);
		initialize();
	}

	private void initialize() throws Exception {
		cf = SolJmsUtility.createConnectionFactory();
		cf.setHost((String) runtimeParamaters.get("BrokerIP"));
		cf.setVPN("default");
		cf.setUsername("clientUsername");
		cf.setPassword("password");
	}

	private Object createConnection() throws JMSException {
		switch ((EndPointType) runtimeParamaters.get("EndpointType")) {
		case Queue: {
			return cf.createQueueConnection();
		}
		case Topic: {
			return cf.createConnection();
		}
		default:
			throw new RuntimeException("No Endpoint defines");
		}
	}

	private Session createSession(Object conn) throws JMSException {
		switch ((EndPointType) runtimeParamaters.get("EndpointType")) {
		case Queue: {
			return ((QueueConnection)conn).createQueueSession(false, SupportedProperty.SOL_CLIENT_ACKNOWLEDGE);
		}
		case Topic: {
			return ((Connection) conn).createSession(false, Session.AUTO_ACKNOWLEDGE);
		}
		default:
			throw new RuntimeException("No Endpoint defines");
		}
	}

	@Override
	public Producer createProducer(Monitor monitor, Endpoint endpoint) throws Exception {
		Producer producer = new SolProducer(monitor, endpoint);
		return producer;
	}

	@Override
	public Consumer createConsumer(Monitor monitor, Endpoint endpoint) throws Exception {
		Consumer consumer = new SolConsumer(monitor, endpoint);
		return consumer;
	}

	@Override
	public Endpoint createEndPoint(String destination) throws Exception {
		Endpoint endpoint = new Endpoint();
		EndPointType endPointType = (EndPointType) runtimeParamaters.get("EndpointType");
		DeliveryMode deliveryMode = (DeliveryMode) runtimeParamaters.get("DeliveryMode");
		Connection connection = (Connection) createConnection();
		Session session = createSession(connection); 
		
		switch (endPointType) {
		case Queue: {
			endpoint.setEndpoint(endPointType, deliveryMode, connection, session, session.createQueue(destination));
		}
			break;
		case Topic: {
			endpoint.setEndpoint(endPointType, deliveryMode, connection, session, session.createTopic(destination));
		}
			break;
		default:
			throw new RuntimeException("No Endpoint defined");
		}
		return endpoint;
	}

}
