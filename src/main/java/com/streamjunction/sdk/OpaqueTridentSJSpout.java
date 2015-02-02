package com.streamjunction.sdk;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.streamjunction.sdk.rmi.OpaquePartitionedTridentSJSpoutCoordinator;
import com.streamjunction.sdk.rmi.OpaquePartitionedTridentSJSpoutEmitter;
import com.streamjunction.sdk.rmi.SJFrameworkConfigurationException;
import com.streamjunction.sdk.rmi.SJTridentSpout;
import com.streamjunction.sdk.rmi.OpaquePartitionedTridentSJSpoutEmitter.ResultAndMetadata;
import com.streamjunction.sdk.rmi.SJSpoutFramework;

public class OpaqueTridentSJSpout implements 
	IOpaquePartitionedTridentSpout<SJTridentSpout.Partitions, SJTridentSpout.Partition, Map>,
	Serializable {
	
	private static Logger logger = LoggerFactory.getLogger(OpaqueTridentSJSpout.class);
	
	private String stream;
	
	public OpaqueTridentSJSpout(String stream) {
		this.stream = stream;
	}
	
	@Override
	public storm.trident.spout.IOpaquePartitionedTridentSpout.Emitter<SJTridentSpout.Partitions, SJTridentSpout.Partition, Map> getEmitter(
			Map conf, TopologyContext context) {
		
		final OpaquePartitionedTridentSJSpoutEmitter emitter;
		try {
			emitter = SJSpoutFramework.Factory.getFramework().getOpaquePartitionedTridentSpoutEmitter(stream, context.getThisTaskIndex());
		} catch (RemoteException e) {
			logger.error("Cannot get emitter", e);
			throw new RuntimeException("Cannot get emitter", e);
		} catch (NotBoundException e) {
			logger.error("Cannot get emitter", e);
			throw new RuntimeException("Cannot get emitter", e);
		} catch (SJFrameworkConfigurationException e) {
			logger.error("Cannot get emitter", e);
			throw new RuntimeException("Cannot get emitter", e);
		}
		
		return new storm.trident.spout.IOpaquePartitionedTridentSpout.Emitter<SJTridentSpout.Partitions, SJTridentSpout.Partition, Map>() {

			@Override
			public Map emitPartitionBatch(
					TransactionAttempt tx,
					TridentCollector collector, 
					SJTridentSpout.Partition partition,
					Map lastPartitionMeta) {
				logger.debug(String.format("sj.emitPartitionMetadata lastPartitionMeta=%s", lastPartitionMeta));
				
				ResultAndMetadata r;
				try {
					r = emitter.emitPartitionBatch(
							tx.getTransactionId(),
							tx.getAttemptId(), 
							partition,
							lastPartitionMeta);
				} catch (RemoteException e) {
					logger.error("Cannot emit partition batch", e);
					return lastPartitionMeta;
				}
				if (r.getValues() != null) {
					for (String v : r.getValues()) {
						collector.emit(new Values(v));
					}
				}
				logger.debug(String.format("sj.emitPartitionBatch metadata=%s", r.getMetadata()));
				return r.getMetadata();
			}

			@Override
			public void refreshPartitions(
					List<SJTridentSpout.Partition> partitionResponsibilities) {
				try {
					emitter.refreshPartitions(partitionResponsibilities);
				} catch (RemoteException e) {
					logger.error("Cannot refresh partitions", e);
				}
			}

			@Override
			public List<SJTridentSpout.Partition> getOrderedPartitions(
					SJTridentSpout.Partitions allPartitionInfo) {
				try {
					return emitter.getOrderedPartitions(allPartitionInfo);
				} catch (RemoteException e) {
					logger.error("Cannot get ordered partitions", e);
					return null;
				}
			}

			@Override
			public void close() {
				try {
					emitter.close();
				} catch (RemoteException e) {
					logger.error("Cannot close", e);
				}
			}
			
		};
	}

	@Override
	public storm.trident.spout.IOpaquePartitionedTridentSpout.Coordinator<SJTridentSpout.Partitions> getCoordinator(
			Map conf, TopologyContext context) {
		
		final OpaquePartitionedTridentSJSpoutCoordinator coord;
		try {
			coord = SJSpoutFramework.Factory.getFramework().getOpaquePartitionedTridentSpoutCoordinator(stream, context.getThisTaskIndex());
		} catch (RemoteException e) {
			logger.error("Cannot get coordinator", e);
			throw new RuntimeException("Cannot get coordinator", e);
		} catch (NotBoundException e) {
			logger.error("Cannot get coordinator", e);
			throw new RuntimeException("Cannot get coordinator", e);
		} catch (SJFrameworkConfigurationException e) {
			logger.error("Cannot get coordinator", e);
			throw new RuntimeException("Cannot get coordinator", e);
		}
		
		return new storm.trident.spout.IOpaquePartitionedTridentSpout.Coordinator<SJTridentSpout.Partitions>() {

			@Override
			public boolean isReady(long txid) {
				try {
					return coord.isReady(txid);
				} catch (RemoteException e) {
					logger.error("Cannot get ready status", e);
					return false;
				}
			}

			@Override
			public SJTridentSpout.Partitions getPartitionsForBatch() {
				try {
					return coord.getPartitionsForBatch();
				} catch (RemoteException e) {
					logger.error("Cannot get partitions for batch", e);
					return null;
				}
			}

			@Override
			public void close() {
				try {
					coord.close();
				} catch (RemoteException e) {
					logger.error("Cannot close", e);
				}
			}
			
		};
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("event");
	}

}
