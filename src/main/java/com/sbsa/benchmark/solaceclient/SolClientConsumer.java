package com.sbsa.benchmark.solaceclient;

import com.sbsa.benchmark.Consumer;
import com.sbsa.benchmark.Endpoint;
import com.sbsa.benchmark.Monitor;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;

public class SolClientConsumer extends Consumer {
	FlowReceiver consumerQueue;
	XMLMessageConsumer consumerTopic;

	public SolClientConsumer(Monitor monitor, Endpoint endpoint) throws Exception {
		super(monitor, endpoint);
		initialise();
	}

	private void initialise() throws Exception {
		switch (endpoint.getEndPointType()) {
		case Queue: {
			final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
			flow_prop.setEndpoint((Queue) endpoint.getEndpoint());
			flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

			EndpointProperties endpoint_props = new EndpointProperties();
			endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

			consumerQueue = ((JCSMPSession) endpoint.getSession()).createFlow(new XMLMessageListener() {
				public void onReceive(final BytesXMLMessage message) {
					message.ackMessage();
					monitor.executor.execute(new Runnable() {
						@Override
						public void run() {
							recordStats(System.nanoTime(), Long.parseLong(message.getCorrelationId()));
						}
					});
				}

				public void onException(JCSMPException e) {

				}
			}, flow_prop, endpoint_props);
		}
			break;
		case Topic: {
			consumerTopic = ((JCSMPSession) endpoint.getSession()).getMessageConsumer(new XMLMessageListener() {
				public void onReceive(final BytesXMLMessage message) {
					monitor.executor.execute(new Runnable() {
						@Override
						public void run() {
							recordStats(System.nanoTime(), Long.parseLong(message.getCorrelationId()));
						}
					});
				}

				public void onException(JCSMPException e) {

				}
			});
			((JCSMPSession) endpoint.getSession()).addSubscription((Topic)endpoint.getEndpoint());
		}
			break;
		default:
			throw new RuntimeException("No end point defined");
		}
	}

	@Override
	public void start() throws Exception {
		switch (endpoint.getEndPointType()) {
		case Queue: {
			consumerQueue.start();
		}
			break;
		case Topic: {
			consumerTopic.start();
		}
			break;
		}
	}
}
