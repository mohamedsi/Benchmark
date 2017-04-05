package com.sbsa.benchmark;

public abstract class Consumer {
	public Long Latency = 0L;
	public Long clientMsgsRx = 0L;
	public Long clientStatsLatencyTotal = 0L;

	protected Monitor monitor;
	protected Endpoint endpoint;

	public Consumer(Monitor monitor, Endpoint endpoint) {
		this.monitor = monitor;
		this.endpoint = endpoint;
	}

	public abstract void start() throws Exception;

	protected void process(Long sentTime) {
		if (monitor.publisherLatch.get()) {
			Latency = (System.nanoTime() - sentTime) / 1000;
			clientMsgsRx++;
			clientStatsLatencyTotal += Latency;
		}
	}

	protected void process(Long arrivalTime, Long sentTime) {
		if (monitor.publisherLatch.get()) {
			Latency = (arrivalTime - sentTime) / 1000;
			clientMsgsRx++;
			clientStatsLatencyTotal += Latency;
		}
	}
	
	protected void recordStats(final long arrivalTime, final long publisherSendTime) {
		this.monitor.executor.execute(new Runnable() {
			@Override
			public void run() {
				// monitor.ConsumerCallback(arrivalTime, publisherSendTime);
			}
		});
	}

}
