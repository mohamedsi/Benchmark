package com.sbsa.benchmark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
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
	protected Snapshot metricsSnapshots = new Snapshot();
	protected Snapshot previousMetricsSnapshots = new Snapshot();
	protected List<Snapshot> snapshots = new ArrayList<Snapshot>();

	private String messagePayload = null;

	private AtomicInteger numberOfSnapShots = new AtomicInteger(0);
	private int numOfMessages;

	public Monitor(ConcurrentHashMap<String, Object> runtimeParamaters, Factory factory) {
		this.runtimeParamaters = runtimeParamaters;
		this.factory = factory;
	}

	public boolean isBusy() {
		return latch.get();
	}

	public void start() {
		latch.set(Boolean.TRUE);
		try {
			System.out.println("Sleeping for 30 seconds to clear all queues");
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			e.printStackTrace();
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
						if (metricsSnapshots.getNumberOfPublishedMessages() < numOfMessages) {
							System.out.println("Snapping Metrics: " + numberOfSnapShots.get());
							Snapshot tempMetricsSnapshots = new Snapshot();
							snap(tempMetricsSnapshots);

							System.out.println("Number Of Messages Published: "
									+ tempMetricsSnapshots.getNumberOfPublishedMessages());
							System.out.println("Published Messages per Second / Producer: "
									+ ((tempMetricsSnapshots.getNumberOfPublishedMessages()
											- previousMetricsSnapshots.getNumberOfPublishedMessages())
											/ producers.size()));

							tempMetricsSnapshots
									.setPublishedMessagesPerSecond(((tempMetricsSnapshots.getNumberOfPublishedMessages()
											- previousMetricsSnapshots.getNumberOfPublishedMessages())
											/ producers.size()));

							storePreviousSnap(tempMetricsSnapshots);
							numberOfSnapShots.incrementAndGet();
						} else {
							export();
							if (metricsSnapshots.getNumberOfPublishedMessages() >= numOfMessages) {
								latch.set(Boolean.FALSE);
							}
						}
					} else if ((SourceType) runtimeParamaters.get("SourceType") == SourceType.Consumer) {
						if (metricsSnapshots.getNumberOfConsumerMessages() > 0) {
							if ((metricsSnapshots.getNumberOfConsumerMessages() / consumers.size()) < numOfMessages) {
								System.out.println("Snapping Metrics: " + numberOfSnapShots.get());
								Snapshot tempMetricsSnapshots = new Snapshot();
								snap(tempMetricsSnapshots);

								System.out.println("Number Of Messages Consumed: "
										+ tempMetricsSnapshots.getNumberOfConsumerMessages());
								System.out.println("Average Pub/Sub Time (ms): "
										+ (((tempMetricsSnapshots.getCumulativeConsumerTime())
												/ tempMetricsSnapshots.getNumberOfConsumerMessages()) * 0.000001));
								System.out.println("Consumed Messages per Second / Consumer: "
										+ ((tempMetricsSnapshots.getNumberOfConsumerMessages()
												- previousMetricsSnapshots.getNumberOfConsumerMessages())
												/ consumers.size()));
								tempMetricsSnapshots.setConsumedMessagesPerSecond(
										((tempMetricsSnapshots.getNumberOfConsumerMessages()
												- previousMetricsSnapshots.getNumberOfConsumerMessages())
												/ consumers.size()));

								storePreviousSnap(tempMetricsSnapshots);
								numberOfSnapShots.incrementAndGet();
							} else {
								export();
							}
						}
					} else {
						if (metricsSnapshots.getNumberOfPublishedMessages() < numOfMessages) {
							if (metricsSnapshots.getNumberOfConsumerMessages() > 0) {
								System.out.println("Snapping Metrics: " + numberOfSnapShots.get());
								Snapshot tempMetricsSnapshots = new Snapshot();
								snap(tempMetricsSnapshots);

								System.out.println("Number Of Messages Published: "
										+ tempMetricsSnapshots.getNumberOfPublishedMessages() + " Consumed: "
										+ tempMetricsSnapshots.getNumberOfConsumerMessages());
								System.out.println("Average Pub/Sub Time (ms): "
										+ (((tempMetricsSnapshots.getCumulativeConsumerTime())
												/ tempMetricsSnapshots.getNumberOfConsumerMessages()) * 0.000001));
								System.out.println("Published Messages per Second / Producer: "
										+ ((tempMetricsSnapshots.getNumberOfPublishedMessages()
												- previousMetricsSnapshots.getNumberOfPublishedMessages())
												/ producers.size()));
								System.out.println("Consumed Messages per Second / Consumer: "
										+ ((tempMetricsSnapshots.getNumberOfConsumerMessages()
												- previousMetricsSnapshots.getNumberOfConsumerMessages())
												/ consumers.size()));

								tempMetricsSnapshots.setPublishedMessagesPerSecond(
										((tempMetricsSnapshots.getNumberOfPublishedMessages()
												- previousMetricsSnapshots.getNumberOfPublishedMessages())
												/ producers.size()));
								tempMetricsSnapshots.setConsumedMessagesPerSecond(
										((tempMetricsSnapshots.getNumberOfConsumerMessages()
												- previousMetricsSnapshots.getNumberOfConsumerMessages())
												/ consumers.size()));

								storePreviousSnap(tempMetricsSnapshots);

								numberOfSnapShots.incrementAndGet();
							}
						} else {
							export();
							if (metricsSnapshots.getNumberOfPublishedMessages() >= numOfMessages) {
								latch.set(Boolean.FALSE);
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			/**
			 * @param tempMetricsSnapshots
			 */
			private void storePreviousSnap(Snapshot tempMetricsSnapshots) {
				previousMetricsSnapshots.setCumulativeConsumerTime(tempMetricsSnapshots.getCumulativeConsumerTime());
				previousMetricsSnapshots
						.setNumberOfConsumerMessages(tempMetricsSnapshots.getNumberOfConsumerMessages());
				previousMetricsSnapshots
						.setCumulativeProducerEndTime(tempMetricsSnapshots.getCumulativeProducerEndTime());
				previousMetricsSnapshots
						.setNumberOfPublishedMessgaes(tempMetricsSnapshots.getNumberOfPublishedMessages());
				previousMetricsSnapshots
						.setCumulativeProducerSendTime(tempMetricsSnapshots.getCumulativeProducerSendTime());

				snapshots.add(tempMetricsSnapshots);
			}

			/**
			 * @param tempMetricsSnapshots
			 */
			private void snap(Snapshot tempMetricsSnapshots) {
				tempMetricsSnapshots.setCumulativeConsumerTime(metricsSnapshots.getCumulativeConsumerTime());
				tempMetricsSnapshots.setNumberOfConsumerMessages(metricsSnapshots.getNumberOfConsumerMessages());
				tempMetricsSnapshots.setCumulativeProducerEndTime(metricsSnapshots.getCumulativeProducerEndTime());
				tempMetricsSnapshots.setNumberOfPublishedMessgaes(metricsSnapshots.getNumberOfPublishedMessages());
				tempMetricsSnapshots.setCumulativeProducerSendTime(metricsSnapshots.getCumulativeProducerSendTime());
			}
		}, 1, 1, TimeUnit.SECONDS);
	}

	private void startPublishers() {
		messagePayload = createPayload();
		numOfMessages = (int) runtimeParamaters.get("NumOfMessages");
		publisherLatch.set(Boolean.TRUE);

		for (final Producer producer : producers) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						while (isBusy() && publisherLatch.get()
								&& metricsSnapshots.getNumberOfPublishedMessages() < numOfMessages) {
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
		if ((SourceType) runtimeParamaters.get("SourceType") == SourceType.Producer
				|| (SourceType) runtimeParamaters.get("SourceType") == SourceType.ProducerConsumer) {
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
		if ((SourceType) runtimeParamaters.get("SourceType") == SourceType.Consumer
				|| (SourceType) runtimeParamaters.get("SourceType") == SourceType.ProducerConsumer) {
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

	public void ConsumerCallback(final long arrivalTime, long publisherSendTime) {
		if (publisherLatch.get()) {
			this.metricsSnapshots.incrementNumberOfConsumerMessages();
			this.metricsSnapshots.incrementCumulativeConsumerTime(arrivalTime - publisherSendTime);
		}
	}

	public void PublisherCallback(final long startTime, final long endTime) {
		if (publisherLatch.get()) {
			this.metricsSnapshots.incrementNumberOfPublishedMessgaes();
			this.metricsSnapshots.incrementCumulativeProducerSendTime(startTime);
			this.metricsSnapshots.incrementCumulativeProducerEndTime(endTime);
		}
	}

	/**
	 * @throws IOException
	 */
	public void export() throws IOException {
		// Stopping
		StringBuffer sb = new StringBuffer();
		sb.append(
				"numPublishedMessages,numConsumedMessages,averagePubSubTime,publishedMessagesPerSecond,consumedMessagesPerSecond,numPublishers,numConsumers");
		sb.append(System.getProperty("line.separator"));
		for (Snapshot snapshot : snapshots) {

			if ((SourceType) runtimeParamaters.get("SourceType") == SourceType.Consumer) {
				sb.append(snapshot.getNumberOfPublishedMessages() + "," + snapshot.getNumberOfConsumerMessages() + ","
						+ (((snapshot.getCumulativeConsumerTime()) / snapshot.getNumberOfConsumerMessages()) * 0.000001)
						+ "," + snapshot.getPublishedMessagesPerSecond() + "," + snapshot.getConsumedMessagesPerSecond()
						+ "," + producers.size() + "," + consumers.size());
			} else if ((SourceType) runtimeParamaters.get("SourceType") == SourceType.Producer) {
				sb.append(snapshot.getNumberOfPublishedMessages() + "," + snapshot.getNumberOfConsumerMessages() + ","
						+ 0 + "," + snapshot.getPublishedMessagesPerSecond() + ","
						+ snapshot.getConsumedMessagesPerSecond() + "," + producers.size() + "," + consumers.size());
			} else {
				sb.append(snapshot.getNumberOfPublishedMessages() + "," + snapshot.getNumberOfConsumerMessages() + ","
						+ (((snapshot.getCumulativeConsumerTime()) / snapshot.getNumberOfConsumerMessages()) * 0.000001)
						+ "," + snapshot.getPublishedMessagesPerSecond() + "," + snapshot.getConsumedMessagesPerSecond()
						+ "," + producers.size() + "," + consumers.size());
			}
			sb.append(System.getProperty("line.separator"));
		}

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
