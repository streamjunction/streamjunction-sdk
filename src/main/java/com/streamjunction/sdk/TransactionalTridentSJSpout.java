package com.streamjunction.sdk;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.spout.ISpoutPartition;

public class TransactionalTridentSJSpout implements 
	IPartitionedTridentSpout<TransactionalTridentSJSpout.Partitions, TransactionalTridentSJSpout.Partition, Map> {

	public static interface Partitions extends Serializable {
		
	}
	
	public static interface Partition extends ISpoutPartition, Serializable {
		
	}

	@Override
	public storm.trident.spout.IPartitionedTridentSpout.Coordinator<Partitions> getCoordinator(
			Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public storm.trident.spout.IPartitionedTridentSpout.Emitter<Partitions, Partition, Map> getEmitter(
			Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return null;
	}

	
}
