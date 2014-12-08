/**
 * 
 */
package com.flipkart.portkey.common.sharding;

import java.util.List;

import com.flipkart.portkey.common.enumeration.DataStore;

/**
 * @author santosh.p
 */
public interface ShardLifeCycleManager
{
	public void activateShard(DataStore ds, String shardId);

	public void deactivateShard(DataStore ds, String shardId);

	public List<String> getActiveShards(DataStore ds);
}
