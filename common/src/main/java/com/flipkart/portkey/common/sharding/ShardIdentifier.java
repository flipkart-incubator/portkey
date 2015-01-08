/**
 * 
 */
package com.flipkart.portkey.common.sharding;

import java.util.List;

import com.flipkart.portkey.common.exception.ShardNotAvailableException;

/**
 * @author santosh.p
 */
public interface ShardIdentifier
{
	public String generateShardId(String shardKey, List<String> liveShards) throws ShardNotAvailableException;

	public String generateNewShardKey(String oldShardKey, String shardId);

	public String getShardId(String shardKey, List<String> liveShards) throws ShardNotAvailableException;
}
