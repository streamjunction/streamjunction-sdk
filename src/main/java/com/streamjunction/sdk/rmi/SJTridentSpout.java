package com.streamjunction.sdk.rmi;

import java.io.Serializable;
import java.util.Map;

import storm.trident.spout.ISpoutPartition;

public class SJTridentSpout {
	
	public static class Partition implements ISpoutPartition, Serializable {

		private final String host;
		private final int port;
		private final int partition;
				
		public Partition(String host, int port, int partition) {
			super();
			this.host = host;
			this.port = port;
			this.partition = partition;
		}

		@Override
		public String getId() {
			return "partition_" + partition;
		}

		public String getHost() {
			return host;
		}

		public int getPort() {
			return port;
		}

		public int getPartition() {
			return partition;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((host == null) ? 0 : host.hashCode());
			result = prime * result + partition;
			result = prime * result + port;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Partition other = (Partition) obj;
			if (host == null) {
				if (other.host != null)
					return false;
			} else if (!host.equals(other.host))
				return false;
			if (partition != other.partition)
				return false;
			if (port != other.port)
				return false;
			return true;
		}
		
		
	}
	
	public static class Partitions implements Serializable {
		
		public static class Node implements Serializable {
			private final String host;
			private final int port;
			public Node(String host, int port) {
				super();
				this.host = host;
				this.port = port;
			}
			public String getHost() {
				return host;
			}
			public int getPort() {
				return port;
			}			
		}
		
		private final Map<Integer, Node> partitions;

		public Partitions(Map<Integer, Node> partitions) {
			super();
			this.partitions = partitions;
		}

		public Map<Integer, Node> getPartitions() {
			return partitions;
		}
				
	}
		

}
