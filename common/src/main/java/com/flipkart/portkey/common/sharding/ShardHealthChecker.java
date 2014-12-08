/**
 * 
 */
package com.flipkart.portkey.common.sharding;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.enumeration.ShardStatus;

/**
 * @author santosh.p
 */
public interface ShardHealthChecker
{
	public Map<String, ShardStatus> healthCheck(List<String> shards);
}
