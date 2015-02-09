package com.flipkart.portkey.rdbms.sharding;

import java.util.List;

import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.common.sharding.ShardIdentifier;

public class RdbmsShardIdentifierForSingleShard implements ShardIdentifier
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
		return "";
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.sharding.ShardIdentifierInterface#getShardId(java.lang.String)
	 */
	public String getShardId(String shardKey, List<String> liveShards) throws ShardNotAvailableException
	{
		if (shardKey == null)
		{
			throw new ShardNotAvailableException("Shard key provided is null");
		}
		return "";
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.sharding.ShardIdentifierInterface#generateNewShardKey(java.lang.String,
	 * java.lang.String)
	 */
	public String generateNewShardKey(String oldShardKey, String shardId)
	{
		return oldShardKey + shardId;
	}
}
