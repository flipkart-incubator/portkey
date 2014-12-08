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
	public String getShardFromShardKey(String key, List<String> liveShards);
}
