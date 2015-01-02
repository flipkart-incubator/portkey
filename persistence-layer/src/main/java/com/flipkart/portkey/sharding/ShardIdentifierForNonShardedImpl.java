/**
 * 
 */
package com.flipkart.portkey.sharding;

import java.util.List;

import com.flipkart.portkey.common.sharding.ShardIdentifierInterface;

/**
 * @author santosh.p
 */
public class ShardIdentifierForNonShardedImpl implements ShardIdentifierInterface
{

	/**
	 * 
	 */
	public ShardIdentifierForNonShardedImpl()
	{
		// TODO Auto-generated constructor stub
	}

	@Override
	public String generateShardId(String shardKey, List<String> liveShards)
	{
		return "";
	}

	@Override
	public String generateNewShardKey(String oldShardKey, String shardId)
	{
		return oldShardKey + shardId;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.sharding.ShardIdentifierInterface#getShardId(java.lang.String)
	 */
	@Override
	public String getShardId(String shardKey)
	{
		return "";
	}

}
