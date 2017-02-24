package com.sbsa.benchmark.solace;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import com.sbsa.benchmark.Endpoint;
import com.sbsa.benchmark.Monitor;
import com.sbsa.benchmark.Producer;

public class SolProducer extends Producer {

	MessageProducer producer;

	public SolProducer(Monitor monitor, Endpoint endpoint) throws Exception {
		super(monitor, endpoint);
		initialise();
	}

	private void initialise() throws JMSException {
		switch (endpoint.getEndPointType()) {
		case Queue: {
			producer = ((Session) endpoint.getSession()).createProducer((Queue) endpoint.getEndpoint());
		}
			break;
		case Topic: {
			producer = ((Session) endpoint.getSession()).createProducer((Topic) endpoint.getEndpoint());
		}
			break;
		default:
			throw new RuntimeException("No end point defined");
		}
	}

	@Override
	public void publish(String message) throws JMSException {
		final long startTime;
		final long endTime;

		int deliveryMode = endpoint.getDeliveryMode() == com.sbsa.benchmark.DeliveryMode.PERSISTENT
				? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;

		TextMessage request = ((Session) endpoint.getSession()).createTextMessage();
		request.setText(message);

		startTime = System.nanoTime();
		request.setJMSCorrelationID(String.valueOf(startTime));

		switch (endpoint.getEndPointType()) {
		case Queue: {

			producer.send((Queue) endpoint.getEndpoint(), (Message) request, deliveryMode, Message.DEFAULT_PRIORITY,
					Message.DEFAULT_TIME_TO_LIVE);
		}
			break;
		case Topic: {
			producer.send((Topic) endpoint.getEndpoint(), (Message) request, deliveryMode, Message.DEFAULT_PRIORITY,
					Message.DEFAULT_TIME_TO_LIVE);
		}
			break;
		default:
			throw new RuntimeException("No end point defined");
		}

		endTime = System.nanoTime();

		this.recordStats(startTime, endTime);
	}

}
