package com.sbsa.benchmark;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class Producer implements Runnable {
	protected LinkedBlockingDeque<Entry<Long, Long>> _stats = new LinkedBlockingDeque<>();
	protected Monitor monitor;
	protected Endpoint endpoint;
	
	public Long clientMsgsTx = 0L;
	

	public Producer(Monitor monitor, Endpoint endpoint) {
		this.monitor = monitor;
		this.endpoint = endpoint;
		initilize();
	}
	

	private void initilize() {
		Thread statsCollector = new Thread(this);
		statsCollector.start();
	}

	public void process(){
		clientMsgsTx++;
	}
	
	public abstract void publish(String Message) throws Exception;

	public void recordStats(final long startTime, final long endTime) {
		_stats.offer(new MyEntry(startTime, endTime));		
	}
	
	@Override
	public void run() {
		
		while(true) {
			Entry<Long, Long> stat;
			try {
				stat = _stats.takeFirst();
				//monitor.PublisherCallback(stat.getKey(), stat.getValue());	
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
	}
	
	final class MyEntry<K, V> implements Map.Entry<K, V> {
	    private final K key;
	    private V value;

	    public MyEntry(K key, V value) {
	        this.key = key;
	        this.value = value;
	    }

	    @Override
	    public K getKey() {
	        return key;
	    }

	    @Override
	    public V getValue() {
	        return value;
	    }

	    @Override
	    public V setValue(V value) {
	        V old = this.value;
	        this.value = value;
	        return old;
	    }
	}
}
