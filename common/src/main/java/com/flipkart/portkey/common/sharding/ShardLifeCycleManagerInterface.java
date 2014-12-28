/**
 * 
 */
package com.flipkart.portkey.common.sharding;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.ShardStatus;

/**
 * @author santosh.p
 */
public interface ShardLifeCycleManagerInterface
{
	public ShardStatus getShardStatus(DataStoreType dataStoreType, String shardId);

	public void setShardStatus(DataStoreType dataStoreType, String shardId, ShardStatus shardStatus);

	public List<String> getShardListForStatus(DataStoreType dataStoreType, ShardStatus shardStatus);

	public Map<String, ShardStatus> getStatusMapForDataStore(DataStoreType dataStoreType);

	public Map<DataStoreType, Map<String, ShardStatus>> getStatus();
}
