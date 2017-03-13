package com.sbsa.benchmark.natsio;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.ArrayList;

import com.sbsa.benchmark.Consumer;
import com.sbsa.benchmark.Endpoint;
import com.sbsa.benchmark.Monitor;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.MessageHandler;

public class NatsIOConsumer extends Consumer {

	Connection con;

	public NatsIOConsumer(Monitor monitor, Endpoint endpoint) throws Exception {
		super(monitor, endpoint);
		initialise();
	}

	private void initialise() throws InterruptedException, IOException {
		con = ((Connection) endpoint.getConnection());
	}

	public Object convertFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
		try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes); ObjectInput in = new ObjectInputStream(bis)) {
			return in.readObject();
		}
	}

	@Override
	public void start() throws Exception {
		con.subscribe((String) endpoint.getEndpoint(), new MessageHandler() {
			public void onMessage(final Message m) {
				monitor.executor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							long arrivalTime = System.nanoTime();
							String message = ((String) convertFromBytes(m.getData()).toString()).replace("[","").replace("]","");
							
							recordStats(arrivalTime, Long.valueOf(message.split(",")[0]));
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
		});
	}
}