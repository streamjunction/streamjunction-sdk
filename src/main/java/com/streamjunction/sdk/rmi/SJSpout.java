package com.streamjunction.sdk.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface SJSpout extends Remote {

	public void open(int totalTasks, int thisTaskIndex, RemoteSpoutOutputCollector collector) throws RemoteException;

	public void close() throws RemoteException;

	public void nextTuple() throws RemoteException;

	public void ack(Object messageId) throws RemoteException;

	// messageId is PartitionManager.KafkaMessageId
	public void fail(Object messageId) throws RemoteException;

	public void deactivate() throws RemoteException;

}