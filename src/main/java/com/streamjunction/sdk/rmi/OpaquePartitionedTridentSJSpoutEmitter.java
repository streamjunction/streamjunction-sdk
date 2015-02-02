package com.streamjunction.sdk.rmi;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

import storm.trident.spout.ISpoutPartition;

public interface OpaquePartitionedTridentSJSpoutEmitter extends Remote {
		
	@SuppressWarnings("rawtypes")
	public static class ResultAndMetadata implements Serializable {

		private static final long serialVersionUID = 1L;
		
		private Map metadata;
		private List<String> values;
		private RemoteException error;
		
		public ResultAndMetadata(Map metadata, List<String> values, RemoteException error) {
			this.metadata = metadata;
			this.values = values;
			this.error = error;
		}
		
		public List<String> getValues() {
			return values;
		}
		
		public Map getMetadata() {
			return metadata;
		}
		
		public RemoteException getError() {
			return error;
		}
	}

	ResultAndMetadata emitPartitionBatch(Long transactionId, int attemptId,
			SJTridentSpout.Partition partition, Map lastPartitionMeta) throws RemoteException;

	void refreshPartitions(List<SJTridentSpout.Partition> partitionResponsibilities) throws RemoteException;

	List<SJTridentSpout.Partition> getOrderedPartitions(SJTridentSpout.Partitions allPartitionInfo) throws RemoteException;

	void close() throws RemoteException;
	
	

}
