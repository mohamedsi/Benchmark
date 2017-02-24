package com.sbsa.benchmark.natsio;

import java.util.concurrent.ConcurrentHashMap;

import com.sbsa.benchmark.Factory;
import com.sbsa.benchmark.Monitor;

public class NatsIOMonitor extends Monitor {

	public NatsIOMonitor(ConcurrentHashMap<String, Object> runtimeParamaters, Factory factory) {
		super(runtimeParamaters, factory);
		
	}

}
