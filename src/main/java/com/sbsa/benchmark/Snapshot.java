package com.sbsa.benchmark;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Snapshot {

	AtomicLong cumulativeProducerStartTime = new AtomicLong();
	AtomicLong cumulativeProducerEndTime = new AtomicLong();
	AtomicLong cumulativeConsumerTime = new AtomicLong();
	
	AtomicInteger numberOfPublishedMessgaes = new AtomicInteger();	
	AtomicInteger numberOfConsumerMessages = new AtomicInteger();

	AtomicInteger publishedMessagesPerSecond = new AtomicInteger();
	AtomicInteger consumedMessagesPerSecond = new AtomicInteger();
	
	public Snapshot() {
	}


	public long getCumulativeProducerSendTime() {
		return this.cumulativeProducerStartTime.get();
	}

	public long getCumulativeProducerEndTime() {
		return this.cumulativeProducerEndTime.get();
	}

	public long getCumulativeConsumerTime() {
		return this.cumulativeConsumerTime.get();
	}
	
	public int getNumberOfPublishedMessages() {
		return this.numberOfPublishedMessgaes.get();
	}
	
	public int getPublishedMessagesPerSecond() {
		return this.publishedMessagesPerSecond.get();
	}
	
	public int getConsumedMessagesPerSecond() {
		return this.consumedMessagesPerSecond.get();
	}
	
	public int getNumberOfConsumerMessages() {
		return this.numberOfConsumerMessages.get();
	}

	public long incrementCumulativeProducerSendTime(final long time) {
		return this.cumulativeProducerStartTime.addAndGet(time);
	}

	public long incrementCumulativeProducerEndTime(final long time) {
		return this.cumulativeProducerEndTime.addAndGet(time);
	}

	public long incrementCumulativeConsumerTime(final long time) {
		return this.cumulativeConsumerTime.addAndGet(time);
	}
	
	public long incrementNumberOfPublishedMessgaes() {
		return this.numberOfPublishedMessgaes.incrementAndGet();
	}
	
	public long incrementNumberOfConsumerMessages() {
		return this.numberOfConsumerMessages.incrementAndGet();
	}	


	public void setCumulativeProducerSendTime(final long time) {
		this.cumulativeProducerStartTime.set(time);
	}

	public void setCumulativeProducerEndTime(final long time) {
		this.cumulativeProducerEndTime.set(time);
	}

	public void setCumulativeConsumerTime(final long time) {
		this.cumulativeConsumerTime.set(time);
	}
	
	public void setNumberOfPublishedMessgaes(final int numberPublish) {
		this.numberOfPublishedMessgaes.set(numberPublish);
	}
	
	public void setNumberOfConsumerMessages(final int numberConsumed) {
		this.numberOfConsumerMessages.set(numberConsumed);
	}	

	public void setPublishedMessagesPerSecond(final int publishedMessagesPerSecond) {
		this.publishedMessagesPerSecond.set(publishedMessagesPerSecond);
	}
	
	public void setConsumedMessagesPerSecond(final int consumedMessagesPerSecond) {
		this.consumedMessagesPerSecond.set(consumedMessagesPerSecond);
	}
}
