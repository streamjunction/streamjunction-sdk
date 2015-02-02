package com.streamjunction.sdk.metrics;

import java.util.Map;

import com.streamjunction.sdk.log.Debug;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.metric.api.IMetric;

public abstract class PrintMetric<Metric extends IMetric> implements Filter {
	
	private String label;
	
	private long interval;
	
	private transient Metric metric;
	
	private transient long timestamp;
	
	public PrintMetric(String label, long interval) {
		this.label = label;
		this.interval = interval;
	}
	
	public abstract Metric createMetric(TridentOperationContext context);
	
	public abstract void updateMetric(Metric metric, TridentTuple tuple);

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		this.timestamp = -1;
		this.metric = createMetric(context);
	}

	@Override
	public void cleanup() {		
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		updateMetric(metric, tuple);
		long now = System.currentTimeMillis();
		if (timestamp >= 0) {
			if (now - timestamp >= interval) {
				Object value = metric.getValueAndReset();
				if (value instanceof Number) {
					Debug.log(String.format("%s: %.2f", label, ((Number)value).doubleValue()));
				} else {
					Debug.log(String.format("%s: %s", label, value));
				}
				timestamp = now;
			}
		} else {
			timestamp = now;
		}
		return true;
	}

}
