package com.sbsa.benchmark.kafka;

import java.util.concurrent.ConcurrentHashMap;

import com.sbsa.benchmark.Factory;
import com.sbsa.benchmark.Monitor;

public class KafkaClientMonitor extends Monitor {

	public KafkaClientMonitor(ConcurrentHashMap<String, Object> runtimeParamaters, Factory factory) {
		super(runtimeParamaters, factory);
		
	}

}
