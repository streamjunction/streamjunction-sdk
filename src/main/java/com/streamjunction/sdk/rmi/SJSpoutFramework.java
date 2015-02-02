package com.streamjunction.sdk.rmi;

import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SJSpoutFramework extends Remote {
	
	public static final String SJ_RMIREGISTRY = "sj.rmi.registry";
	
	public SJSpout getSpout(String streamName, int index) throws RemoteException;
	
	public OpaquePartitionedTridentSJSpoutEmitter getOpaquePartitionedTridentSpoutEmitter(
			String streamName, int index) throws RemoteException; 
	
	public OpaquePartitionedTridentSJSpoutCoordinator getOpaquePartitionedTridentSpoutCoordinator(
			String streamName, int index) throws RemoteException;
	
	public String getProperty(String key) throws RemoteException;

	public static class Factory {
		
		private static Logger logger = LoggerFactory.getLogger(SJSpoutFramework.Factory.class);
		
		private static SJSpoutFramework fwk = null;
		
		public static synchronized SJSpoutFramework getFramework() 
				throws RemoteException, NotBoundException, SJFrameworkConfigurationException {
			if (fwk == null) {			
				String hostPort = System.getProperty(SJ_RMIREGISTRY);
				
				if (hostPort == null) {
					throw new SJFrameworkConfigurationException("SJ RMI Registry address is not provided. See system property "+SJ_RMIREGISTRY);
				}
				
				int i = hostPort.indexOf(':');
				
				String host;
				int port;
				
				if (i == -1) {
					host = hostPort;
					port = 6000;
				} else {
					host = hostPort.substring(0, i);
					try {
						port = Integer.parseInt(hostPort.substring(i+1));
					} catch (NumberFormatException _) {
						port = 6000;
					}
				}
				
				logger.info(String.format("Connecting to SJSpoutFramework at %s:%d", host, port));
				
				Registry registry = LocateRegistry.getRegistry(host, port);
				fwk = (SJSpoutFramework) registry.lookup(SJSpoutFramework.class.getName());
				return fwk;
			} else {
				return fwk;
			}
		}
		
	}

}
