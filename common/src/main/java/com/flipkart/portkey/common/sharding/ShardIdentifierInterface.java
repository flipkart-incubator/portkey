/**
 * 
 */
package com.flipkart.portkey.common.sharding;

import java.util.List;

/**
 * @author santosh.p
 */
public interface ShardIdentifierInterface
{
	public String generateShardId(String shardKey, List<String> liveShards);

	public String generateNewShardKey(String oldShardKey, String shardId);

	public String getShardId(String shardKey);
}
