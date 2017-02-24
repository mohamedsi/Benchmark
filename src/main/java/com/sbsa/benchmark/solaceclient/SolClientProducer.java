package com.sbsa.benchmark.solaceclient;

import com.sbsa.benchmark.EndPointType;
import com.sbsa.benchmark.Endpoint;
import com.sbsa.benchmark.Monitor;
import com.sbsa.benchmark.Producer;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class SolClientProducer extends Producer {

	XMLMessageProducer producer;
	TextMessage request;
	Queue queue;
	Topic topic;
	EndPointType endPointType;
	DeliveryMode deliveryMode;

	public SolClientProducer(Monitor monitor, Endpoint endpoint) throws Exception {
		super(monitor, endpoint);
		initialise();
	}

	private void initialise() throws JCSMPException {
		switch (endpoint.getEndPointType()) {
		case Queue: {
			producer = ((JCSMPSession) endpoint.getSession())
					.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
						public void responseReceived(String messageID) {

						}

						public void handleError(String messageID, JCSMPException e, long timestamp) {
						}
					});
		}
			break;
		case Topic: {
			producer = ((JCSMPSession) endpoint.getSession())
					.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
						public void responseReceived(String messageID) {

						}

						public void handleError(String messageID, JCSMPException e, long timestamp) {

						}
					});
		}
			break;
		default:
			throw new RuntimeException("No end point defined");
		}
	}

	@Override
	public void publish(String message) throws Exception {
		final long startTime;
		final long endTime;

		if (request == null) {
			deliveryMode = endpoint.getDeliveryMode() == com.sbsa.benchmark.DeliveryMode.PERSISTENT
					? DeliveryMode.PERSISTENT : DeliveryMode.DIRECT;
			request = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			request.setText(message);
			request.setDeliveryMode(deliveryMode);
			endPointType = endpoint.getEndPointType();

			if (endPointType == EndPointType.Queue) {
				queue = (Queue) endpoint.getEndpoint();
			} else if (endPointType == EndPointType.Topic) {
				topic = (Topic) endpoint.getEndpoint();
			}
		}

		startTime = System.nanoTime();
		request.setCorrelationId(String.valueOf(startTime));

		switch (endPointType) {
		case Queue: {
			producer.send(request, queue);
		}
			break;
		case Topic: {
			producer.send(request, topic);
		}
			break;
		default:
			throw new RuntimeException("No end point defined");
		}

		endTime = System.nanoTime();

		this.recordStats(startTime, endTime);
	}

}
