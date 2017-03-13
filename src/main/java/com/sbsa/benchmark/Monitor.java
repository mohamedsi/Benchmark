package com.sbsa.benchmark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Monitor {
	public ExecutorService executor = Executors.newWorkStealingPool();
	// public ExecutorService executor = Executors.newCachedThreadPool();
	public ExecutorService cacheThreadPoolexecutor = Executors.newCachedThreadPool();
	public ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
	public ConcurrentHashMap<String, Object> runtimeParamaters;
	protected ArrayList<String> destinations = new ArrayList<String>();
	protected ArrayList<Endpoint> endPoints = new ArrayList<Endpoint>();
	protected ArrayList<Producer> producers = new ArrayList<Producer>();
	protected ArrayList<Consumer> consumers = new ArrayList<Consumer>();
	protected Factory factory;
	protected AtomicBoolean latch = new AtomicBoolean(Boolean.FALSE);
	protected AtomicBoolean publisherLatch = new AtomicBoolean(Boolean.FALSE);

	Long[] cons1total;
	Long bestMean = 0L;
	Long worstMean = 0L;

	private String messagePayload = null;

	StringBuffer sb = new StringBuffer();

	public Monitor(ConcurrentHashMap<String, Object> runtimeParamaters, Factory factory) {
		this.runtimeParamaters = runtimeParamaters;
		this.factory = factory;
		if ((SourceType) runtimeParamaters.get("SourceType") == SourceType.Producer) {
			sb.append("publishedMessagesPerSecond,numPublishers");
			sb.append(System.getProperty("line.separator"));
		} else {
			sb.append("consumedMessagesPerSecond,currentMean,bestMean,worstMean,numConsumers");
			sb.append(System.getProperty("line.separator"));
		}
	}

	public boolean isBusy() {
		return latch.get();
	}

	public void start() {
		latch.set(Boolean.TRUE);
		try {
			if ((SourceType) runtimeParamaters.get("SourceType") == SourceType.Consumer) {
				long sleep = 30000L;
				System.out.println("Sleeping for "+String.valueOf(sleep/1000)+" seconds to clear all queues");
				Thread.sleep(sleep);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if ((SourceType) runtimeParamaters.get("SourceType") == SourceType.Producer) {
			cons1total = new Long[producers.size()];
			for (int i = 0; i < producers.size(); i++) {
				cons1total[i] = 0L;
			}

		} else {
			cons1total = new Long[consumers.size()];
			for (int i = 0; i < consumers.size(); i++) {
				cons1total[i] = 0L;
			}
		}

		startPublishers();
		startMetricsMonitor();
	}

	private void startMetricsMonitor() {
		scheduledExecutor.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					if ((SourceType) runtimeParamaters.get("SourceType") == SourceType.Producer) {
						int idx = 0;
						Long aggRate = 0L;
						Long tempMean = 0L;
						Producer producerTest;

						Iterator<Producer> iterator = producers.listIterator();

						while (iterator.hasNext()) {
							producerTest = iterator.next();
							aggRate += (producerTest.clientMsgsTx - cons1total[idx]);
							cons1total[idx] = producerTest.clientMsgsTx;
							idx++;
						}
						System.out.println("Aggregate Publisher Rate : RX = " + (aggRate / producers.size()));
						sb.append(String.valueOf(aggRate / producers.size()) + "," + producers.size());
						sb.append(System.getProperty("line.separator"));
					} else {
						int idx = 0;
						Long aggRate = 0L;
						Long tempMean = 0L;
						Consumer consumerTest;

						Iterator<Consumer> iterator = consumers.listIterator();

						while (iterator.hasNext()) {
							consumerTest = iterator.next();
							if (consumerTest.clientMsgsRx > 0) {
								tempMean += consumerTest.clientStatsLatencyTotal / consumerTest.clientMsgsRx;
								aggRate += (consumerTest.clientMsgsRx - cons1total[idx]);
								cons1total[idx] = consumerTest.clientMsgsRx;
								idx++;
							}
						}
						tempMean = tempMean / consumers.size();

						if (worstMean == 0l) {
							worstMean = tempMean;
						} else if (tempMean > worstMean) {
							worstMean = tempMean;
						}

						if (bestMean == 0L) {
							bestMean = tempMean;
						} else if (bestMean > tempMean) {
							bestMean = tempMean;
						}

						System.out.println("Aggregate Subscriber Rate : RX = " + (aggRate / consumers.size())
								+ ", (Current) Mean = " + tempMean + "us, (Best) Mean = " + bestMean
								+ "us, (Worst) Mean = " + worstMean + "us");
						sb.append(String.valueOf(aggRate / consumers.size()) + "," + String.valueOf(tempMean) + ","
								+ String.valueOf(bestMean) + "," + String.valueOf(worstMean) + "," + consumers.size());
						sb.append(System.getProperty("line.separator"));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, 1, 1, TimeUnit.SECONDS);

	}

	private void startPublishers() {
		messagePayload = createPayload();
		publisherLatch.set(Boolean.TRUE);

		for (final Producer producer : producers) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						while (isBusy() && publisherLatch.get()) {
							try {
								producer.publish(messagePayload);
							} catch (Exception e) {
								System.out.println(e.getMessage());
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
		}
	}

	// randomise the chars??
	private String createPayload() {
		Double msgSize = new Double((double) runtimeParamaters.get("PayLoadSize"));
		// Java chars are 2 bytes
		msgSize = msgSize / 2;
		msgSize = msgSize * 1024;
		StringBuilder sb = new StringBuilder(msgSize.intValue());
		for (int i = 0; i < msgSize; i++) {
			sb.append('a');
		}
		return sb.toString();
	}

	protected void createEndPoints() throws Exception {
		endPoints.clear();
		int numberOfEndpoints = (int) runtimeParamaters.get("NumberOfEndpoints");
		int numberOfProducers = (int) runtimeParamaters.get("NumProducers");

		for (int i = 0; i < numberOfProducers; i++) {
			for (int j = 0; j < numberOfEndpoints; j++) {
				String endpoint = "BenchMark/Tests/Producer" + i + "/EndPoint" + j;
				destinations.add(endpoint);
				System.out.println("Created Destinations : " + endpoint);
			}
		}
	}

	protected void createProducers() throws Exception {
		if ((SourceType) runtimeParamaters.get("SourceType") == SourceType.Producer) {
			producers.clear();
			for (String destination : destinations) {
				Endpoint endpoint = factory.createEndPoint(destination);
				endPoints.add(endpoint);
				producers.add(factory.createProducer(this, endpoint));
				System.out.println("Created Producer for Endpoint : " + destination);
			}
		}
	}

	protected void createConsumers() throws Exception {
		if ((SourceType) runtimeParamaters.get("SourceType") == SourceType.Consumer) {
			consumers.clear();
			int numberOfConsumers = (int) runtimeParamaters.get("NumConsumers");

			for (String destination : destinations) {
				for (int i = 0; i < numberOfConsumers; i++) {
					Endpoint endpoint = factory.createEndPoint(destination);
					endPoints.add(endpoint);
					consumers.add(factory.createConsumer(this, endpoint));
					System.out.println("Created Consumer for Endpoint : " + destination);
				}
			}
		}
	}

	protected void startConsumers() throws Exception {
		for (Consumer consumer : consumers) {
			consumer.start();
		}
	}

	/**
	 * @throws IOException
	 */
	public void export() throws IOException {
		// Stopping
		BufferedWriter bwr = new BufferedWriter(new FileWriter(
				new File("./" + ((Implementation) runtimeParamaters.get("Implmentation")).toString() + ".csv")));
		bwr.write(sb.toString());
		bwr.flush();
		bwr.close();
	}

	public void stop() {
		latch.set(Boolean.FALSE);
	}
}
