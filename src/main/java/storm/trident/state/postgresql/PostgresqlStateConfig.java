/**
 * Borrowed from Dave Katten's https://github.com/geoforce/storm-postgresql
 * 
 * [No license as of 12/20/2014]
 * 
 * 
 */
package storm.trident.state.postgresql;


import java.io.Serializable;

import storm.trident.state.StateType;

@SuppressWarnings("serial")
public class PostgresqlStateConfig implements Serializable {

	private String table;
	private StateType type = StateType.OPAQUE;
	private String[] keyColumns;
	private String[] valueColumns;
	private int batchSize = DEFAULT_BATCH_SIZE;
	private int cacheSize = DEFAULT_CACHE_SIZE;

	private static final int DEFAULT_CACHE_SIZE = 5000;
	private static final int DEFAULT_BATCH_SIZE = 5000;

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public StateType getType() {
		return type;
	}

	public void setType(StateType type) {
		this.type = type;
	}

	public String[] getKeyColumns() {
		return keyColumns;
	}

	public void setKeyColumns(String... keyColumns) {
		this.keyColumns = keyColumns;
	}

	public int getCacheSize() {
		return cacheSize;
	}

	public void setCacheSize(int cacheSize) {
		this.cacheSize = cacheSize;
	}

	public String[] getValueColumns() {
		return valueColumns;
	}

	public void setValueColumns(String... valueColumns) {
		this.valueColumns = valueColumns;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

}