/**
 * 
 */
package com.flipkart.portkey.common.sharding;

import java.util.List;

/**
 * @author santosh.p
 */
public interface ShardIdentifier
{
	public String generateShardId(String shardKey, List<String> liveShards);

	public String getShardId(String shardKey);
}
