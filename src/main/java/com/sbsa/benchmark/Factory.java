package com.sbsa.benchmark;

import java.util.concurrent.ConcurrentHashMap;

public abstract class Factory {
	protected ConcurrentHashMap<String, Object> runtimeParamaters;
	
	public Factory (ConcurrentHashMap<String, Object> runtimeParamaters){
		this.runtimeParamaters = runtimeParamaters;
	}
	
	public abstract Endpoint createEndPoint(String destination) throws Exception;
	public abstract Producer createProducer(Monitor monitor, Endpoint endpoint) throws Exception;
	public abstract Consumer createConsumer(Monitor monitor,Endpoint endpoint) throws Exception;

	
}
