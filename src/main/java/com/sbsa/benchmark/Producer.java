package com.sbsa.benchmark;

public abstract class Producer {
	protected Monitor monitor;
	protected Endpoint endpoint;

	public Producer(Monitor monitor, Endpoint endpoint) {
		this.monitor = monitor;
		this.endpoint = endpoint;
	}

	public abstract void publish(String Message) throws Exception;

	public void recordStats(final long startTime, final long endTime) {

		this.monitor.executor.execute(new Runnable() {
			@Override
			public void run() {
				monitor.PublisherCallback(startTime, endTime);
			}
		});
	}
}
