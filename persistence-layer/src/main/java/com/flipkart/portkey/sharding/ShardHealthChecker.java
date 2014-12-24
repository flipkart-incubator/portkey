/**
 * 
 */
package com.flipkart.portkey.sharding;

import java.util.HashMap;
import java.util.Map;

import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.persistence.PersistenceManager;
import com.flipkart.portkey.common.sharding.ShardHealthCheckerInterface;

/**
 * @author santosh.p
 */
public class ShardHealthChecker implements ShardHealthCheckerInterface
{

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.sharding.ShardHealthChecker#healthCheck(java.util.List)
	 */
	public Map<String, ShardStatus> healthCheck(Map<String, PersistenceManager> persistenceManagerMap)
	{
		Map<String, ShardStatus> shardStatusMap = new HashMap<String, ShardStatus>();
		for (String key : persistenceManagerMap.keySet())
		{
			PersistenceManager persistenceManager = persistenceManagerMap.get(key);
			ShardStatus shardStatus = persistenceManager.healthCheck();
			shardStatusMap.put(key, shardStatus);
		}
		return shardStatusMap;
	}
}
