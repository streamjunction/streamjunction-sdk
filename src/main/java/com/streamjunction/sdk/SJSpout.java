package com.streamjunction.sdk;

import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import com.streamjunction.sdk.rmi.RemoteSpoutOutputCollector;
import com.streamjunction.sdk.rmi.SJFrameworkConfigurationException;
import com.streamjunction.sdk.rmi.SJSpoutFramework;

public class SJSpout extends BaseRichSpout {
	
	private String streamName;
	private transient com.streamjunction.sdk.rmi.SJSpout spoutSpi;
	private transient RemoteSpoutOutputCollector remoteCollector;
	private transient static Logger logger = LoggerFactory.getLogger(SJSpout.class);

	public SJSpout(String streamName) {
		this.streamName = streamName;
	}

	public void open(
			Map conf, 
			TopologyContext context,
			final SpoutOutputCollector collector) {
		try {
			spoutSpi = SJSpoutFramework.Factory.getFramework().getSpout(streamName, context.getThisTaskIndex());
		} catch (RemoteException e) {
			logger.error("Cannot open spout", e);
			throw new RuntimeException("Cannot open spout", e);
		} catch (NotBoundException e) {
			logger.error("Cannot open spout", e);
			throw new RuntimeException("Cannot open spout", e);
		} catch (SJFrameworkConfigurationException e) {
			logger.error("Cannot open spout", e);
			throw new RuntimeException("Cannot open spout", e);
		}
		remoteCollector = new RemoteSpoutOutputCollector() {			
			@Override
			public void reportError(Throwable error) throws RemoteException {
				collector.reportError(error);
			}
			
			@Override
			public void emitDirect(int taskId, String streamId, List<Object> tuple,
					Object messageId) throws RemoteException {
				collector.emitDirect(taskId, streamId, tuple, messageId);
			}
			
			@Override
			public List<Integer> emit(String streamId, List<Object> tuple,
					Object messageId) throws RemoteException {
				return collector.emit(streamId, tuple, messageId);
			}
		};
		RemoteSpoutOutputCollector remoteCollectorStub;
		try {
			remoteCollectorStub = (RemoteSpoutOutputCollector) 
					UnicastRemoteObject.exportObject(remoteCollector, 0);
		} catch (RemoteException e) {
			logger.error("Cannot open spout", e);
			throw new RuntimeException("Cannot open spout", e);
		}
		int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
		int thisTaskIndex = context.getThisTaskIndex();
		try {
			spoutSpi.open(totalTasks, thisTaskIndex, remoteCollectorStub);
		} catch (RemoteException e) {
			logger.error("Cannot open spout", e);
			throw new RuntimeException("Cannot open spout", e);
		}
	}

	public void nextTuple() {
		try {
			spoutSpi.nextTuple();
		} catch (RemoteException e) {
			// ignore
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("event"));
	}

	@Override
	public void close() {
		if (spoutSpi != null) {
			try {
				spoutSpi.close();
			} catch (RemoteException e) {
				logger.error("Error closing spout spi", e);
			}
			spoutSpi = null;
		}
		if (remoteCollector != null) {
			try {
				UnicastRemoteObject.unexportObject(remoteCollector, true);
			} catch (NoSuchObjectException e) {
				logger.error("Cannot unexport object", e);
			}
			remoteCollector = null;
		}
	}

	@Override
	public void deactivate() {
		try {
			spoutSpi.deactivate();
		} catch (RemoteException e) {
			logger.error("Cannot deactivate", e);
		}
	}

	@Override
	public void ack(Object msgId) {
		try {
			spoutSpi.ack(msgId);
		} catch (RemoteException e) {
			logger.error("Cannot ack", e);
		}
	}

	@Override
	public void fail(Object msgId) {
		try {
			spoutSpi.fail(msgId);
		} catch (RemoteException e) {
			logger.error("Cannot fail", e);
		}
	}
	
	
	
}
