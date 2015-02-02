package com.streamjunction.sdk.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface OpaquePartitionedTridentSJSpoutCoordinator extends Remote {
	
	boolean isReady(long txid) throws RemoteException;

	SJTridentSpout.Partitions getPartitionsForBatch() throws RemoteException;

	void close() throws RemoteException;

}
