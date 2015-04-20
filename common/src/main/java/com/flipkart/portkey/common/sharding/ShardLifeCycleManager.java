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
public interface ShardLifeCycleManager
{
	public ShardStatus getShardStatus(DataStoreType dataStoreType, String databaseName, String shardId);

	public void setShardStatus(DataStoreType dataStoreType, String databaseName, String shardId, ShardStatus shardStatus);

	public void setShardStatusMap(DataStoreType dataStoreType, String databaseName,
	        Map<String, ShardStatus> shardStatusMap);

	public List<String> getShardListForStatus(DataStoreType dataStoreType, String databaseName, ShardStatus shardStatus);

	public Map<String, ShardStatus> getShardStatusMap(DataStoreType dataStoreType, String databaseName);
}
