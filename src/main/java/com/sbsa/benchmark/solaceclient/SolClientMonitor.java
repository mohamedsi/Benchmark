package com.sbsa.benchmark.solaceclient;

import java.util.concurrent.ConcurrentHashMap;

import com.sbsa.benchmark.Factory;
import com.sbsa.benchmark.Monitor;

public class SolClientMonitor extends Monitor {

	public SolClientMonitor(ConcurrentHashMap<String, Object> runtimeParamaters, Factory factory) {
		super(runtimeParamaters, factory);
		
	}

}
