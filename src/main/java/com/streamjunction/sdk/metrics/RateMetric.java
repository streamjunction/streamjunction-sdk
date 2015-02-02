package com.streamjunction.sdk.metrics;

import backtype.storm.metric.api.IMetric;

public class RateMetric implements IMetric {
	
	private long timestamp = -1;
	
	private double value = 0;
	
	private double scale = 1.0;
	
	public RateMetric(double scale) {
		this.scale = scale;
	}
	
	public synchronized void incrBy(double incr) {
		value += incr;
	}
		
	@Override
	public synchronized Object getValueAndReset() {
		long now = System.currentTimeMillis();
		if (timestamp != -1) {
			double result = value * scale / (now - timestamp);
			timestamp = now;
			value = 0;
			return result;
		} else {
			timestamp = now;
			value = 0;
			return (double) 0;
		}
	}
	

	

}
