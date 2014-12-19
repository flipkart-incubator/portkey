/**
 * 
 */
package com.flipkart.portkey.common.sharding;

import java.util.Map;

import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.persistence.PersistenceManager;

/**
 * @author santosh.p
 */
public interface ShardHealthCheckerInterface
{
	public Map<String, ShardStatus> healthCheck(Map<String, PersistenceManager> persistenceManagerMap);
}
