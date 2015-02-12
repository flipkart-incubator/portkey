/**
 * 
 */
package com.flipkart.portkey.redis.datastore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.datastore.DataStoreConfig;
import com.flipkart.portkey.common.persistence.PersistenceManager;
import com.flipkart.portkey.common.sharding.ShardIdentifier;
import com.flipkart.portkey.redis.metadata.RedisMetaDataCache;
import com.flipkart.portkey.redis.sharding.RedisShardIdentifier;

/**
 * @author santosh.p
 */
public class RedisDataStore implements DataStoreConfig
{

	private Map<String, PersistenceManager> shardIdToPersistenceManagerMap;
	private RedisMetaDataCache metaDataCache;
	private ShardIdentifier shardIdentifier = new RedisShardIdentifier();

	public RedisDataStore()
	{

	}

	public RedisDataStore(Map<String, PersistenceManager> shardIdToPersistenceManagerMap,
	        RedisMetaDataCache metaDataCache)
	{
		this.shardIdToPersistenceManagerMap = shardIdToPersistenceManagerMap;
		this.metaDataCache = metaDataCache;
	}

	public void setShardIdToPersistenceManagerMap(Map<String, PersistenceManager> shardIdToPersistenceManagerMap)
	{
		this.shardIdToPersistenceManagerMap = shardIdToPersistenceManagerMap;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.datastore.DataStore#getPersistenceManager(java.lang.String)
	 */
	public PersistenceManager getPersistenceManager(String shardId)
	{
		return shardIdToPersistenceManagerMap.get(shardId);
	}

	public RedisMetaDataCache getMetaDataCache()
	{
		return metaDataCache;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.datastore.DataStore#getShardIds()
	 */
	public List<String> getShardIds()
	{
		return new ArrayList<String>(shardIdToPersistenceManagerMap.keySet());
	}

	public ShardIdentifier getShardIdentifier()
	{
		return shardIdentifier;
	}

	public void setShardIdentifier(ShardIdentifier shardIdentifier)
	{
		this.shardIdentifier = shardIdentifier;
	}

	public void setMetaDataCache(RedisMetaDataCache metaDataCache)
	{
		this.metaDataCache = metaDataCache;
	}
}
