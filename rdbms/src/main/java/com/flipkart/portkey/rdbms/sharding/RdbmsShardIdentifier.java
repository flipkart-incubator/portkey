/**
 * 
 */
package com.flipkart.portkey.rdbms.sharding;

import java.util.List;

import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.common.sharding.ShardIdentifier;

/**
 * @author santosh.p
 */
public class RdbmsShardIdentifier implements ShardIdentifier
{
	public String generateShardId(String shardKey, List<String> liveShards) throws ShardNotAvailableException
	{
		if (shardKey == null)
		{
			throw new ShardNotAvailableException("Shard key provided is null");
		}
		if (liveShards == null || liveShards.size() == 0)
		{
			throw new ShardNotAvailableException("No live shard is available");
		}
		int shardId = Math.abs(shardKey.hashCode()) % liveShards.size();
		return liveShards.get(shardId);
	}

	public String getShardId(String shardKey, List<String> liveShards) throws ShardNotAvailableException
	{
		if (shardKey == null)
		{
			throw new ShardNotAvailableException("Shard key provided is null");
		}
		return shardKey.substring(shardKey.length() - 2);
	}

	public String generateNewShardKey(String oldShardKey, String shardId)
	{
		return oldShardKey + shardId;
	}
}
