/**
 * 
 */
package com.flipkart.portkey.sharding;

import java.util.List;

import com.flipkart.portkey.common.sharding.ShardIdentifierInterface;

/**
 * @author santosh.p
 */
public class ShardIdentifier implements ShardIdentifierInterface
{
	// private int shardIdLength = 2;
	// private int shardKeyLength = 16;

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.sharding.ShardIdentifierInterface#generateShardId(java.lang.String,
	 * java.util.List)
	 */

	public ShardIdentifier()
	{

	}

	public String generateShardId(String shardKey, List<String> liveShards)
	{
		if (shardKey == null || liveShards == null)
		{
			return null;
		}
		int shardId = Math.abs(shardKey.hashCode() % liveShards.size() + 1);
		if (shardId < 10)
		{
			return "0" + shardId;
		}
		return Integer.toString(shardId);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.sharding.ShardIdentifierInterface#getShardId(java.lang.String)
	 */
	public String getShardId(String shardKey)
	{
		if (shardKey == null)
		{
			return null;
		}
		return shardKey.substring(shardKey.length() - 2);
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
