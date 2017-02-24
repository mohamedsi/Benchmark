package com.sbsa.benchmark.natsio;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.sbsa.benchmark.Endpoint;
import com.sbsa.benchmark.Monitor;
import com.sbsa.benchmark.Producer;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;


public class NatsIOProducer extends Producer {
	
	private static final SecureRandom random = new SecureRandom();
	
	Connection con;
	
	ConnectionFactory cf = new ConnectionFactory();
	Connection sc = cf.createConnection();
	
	public NatsIOProducer(Monitor monitor, Endpoint endpoint) throws Exception {
		super(monitor, endpoint);
		initialise();
	}
	

	private void initialise() {
		con = ((Connection) endpoint.getConnection());
		

	}

	
	@Override
	public void publish(String message) throws IOException {
		final long startTime;
		final long endTime;
		
		
		List<Object> objectList = new ArrayList<Object>();
		
		startTime = System.nanoTime();
		
		objectList.add(startTime);
		objectList.add(message);
		
		con.publish((String)endpoint.getEndpoint(),convertToBytes(objectList));
		endTime = System.nanoTime();
		
		this.recordStats(startTime, endTime);
	}
	
	private byte[] convertToBytes(Object object) throws IOException {
	    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
	         ObjectOutput out = new ObjectOutputStream(bos)) {
	        out.writeObject(object);
	        return bos.toByteArray();
	    } 
	}

}
