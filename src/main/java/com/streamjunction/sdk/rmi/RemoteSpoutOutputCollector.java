package com.streamjunction.sdk.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface RemoteSpoutOutputCollector extends Remote {
	
	List<Integer> emit(String streamId, List<Object> tuple, Object messageId) throws RemoteException;
    void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) throws RemoteException;
    void reportError(Throwable error) throws RemoteException;

}
