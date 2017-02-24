package com.sbsa.benchmark.solace;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import com.sbsa.benchmark.Consumer;
import com.sbsa.benchmark.Endpoint;
import com.sbsa.benchmark.Monitor;

public class SolConsumer extends Consumer implements MessageListener {
	MessageConsumer consumer;

	public SolConsumer(Monitor monitor, Endpoint endpoint) throws Exception {
		super(monitor, endpoint);
		initialise();
	}

	private void initialise() throws JMSException {
		switch (endpoint.getEndPointType()) {
		case Queue: {
			consumer = ((Session) endpoint.getSession()).createConsumer((Queue) endpoint.getEndpoint());
		}
			break;
		case Topic: {
			consumer = ((Session) endpoint.getSession()).createConsumer((Topic) endpoint.getEndpoint());
		}
			break;
		default:
			throw new RuntimeException("No end point defined");
		}

		consumer.setMessageListener(this);
	}

	@Override
	public void start() throws Exception {
		((Connection) endpoint.getConnection()).start();
	}

	@Override
	public void onMessage(final Message message) {
		this.monitor.executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					recordStats(System.nanoTime(), Long.parseLong(message.getJMSCorrelationID()));
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		});
	}

}
