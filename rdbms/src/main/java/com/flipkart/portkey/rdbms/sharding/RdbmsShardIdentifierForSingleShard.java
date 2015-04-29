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

	public String getShardId(String shardKey, List<String> liveShards) throws ShardNotAvailableException
	{
		if (shardKey == null)
		{
			throw new ShardNotAvailableException("Shard key provided is null");
		}
		return "";
	}

	public String generateNewShardKey(String oldShardKey, String shardId)
	{
		return oldShardKey + shardId;
	}
}
