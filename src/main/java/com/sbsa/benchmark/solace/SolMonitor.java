package com.sbsa.benchmark.solace;

import java.util.concurrent.ConcurrentHashMap;

import com.sbsa.benchmark.Factory;
import com.sbsa.benchmark.Monitor;

public class SolMonitor extends Monitor {

	public SolMonitor(ConcurrentHashMap<String, Object> runtimeParamaters, Factory factory) {
		super(runtimeParamaters, factory);
		
	}

}
