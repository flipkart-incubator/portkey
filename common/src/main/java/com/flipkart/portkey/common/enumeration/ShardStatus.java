/**
 * 
 */
package com.flipkart.portkey.common.enumeration;

/**
 * Represents status of a shard.
 * 1. Available for write - The shard is healthy and both reads as well as writes can be performed.
 * 2. Available for read - Master of shard is down so writes can not be performed but at least one slave is up so reads
 * can still be performed.
 * 3. Unavailable - All the insatnces in shard are down hence neither reads nor writes can be performed.
 * 4. Disabled - The shard is disabled for reasons like maintenance.
 * @author santosh.p
 */
public enum ShardStatus
{
	AVAILABLE_FOR_WRITE, AVAILABLE_FOR_READ, UNAVAILABLE, DISABLED;
}
