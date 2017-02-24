package com.sbsa.benchmark;

public abstract class Consumer {
	protected Monitor monitor;
	protected Endpoint endpoint;

	public Consumer(Monitor monitor, Endpoint endpoint) {
		this.monitor = monitor;
		this.endpoint = endpoint;
	}

	public abstract void start() throws Exception;


	protected void recordStats(final long arrivalTime, final long publisherSendTime) {
		this.monitor.executor.execute(new Runnable() {
			@Override
			public void run() {
				monitor.ConsumerCallback(arrivalTime, publisherSendTime);
			}
		});		
	}

}
