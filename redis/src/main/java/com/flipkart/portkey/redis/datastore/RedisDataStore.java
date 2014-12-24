/**
 * 
 */
package com.flipkart.portkey.redis.datastore;

import java.util.Map;

import com.flipkart.portkey.common.datastore.AbstractDataStore;
import com.flipkart.portkey.common.metadata.MetaDataCache;
import com.flipkart.portkey.common.persistence.PersistenceManager;
import com.flipkart.portkey.redis.metadata.RedisMetaDataCache;

/**
 * @author santosh.p
 */
public class RedisDataStore extends AbstractDataStore
{

	private RedisMetaDataCache metaDataCache;

	public RedisDataStore(Map<String, PersistenceManager> shardIdToPersistenceManagerMap,
	        RedisMetaDataCache metaDataCache)
	{
		super(shardIdToPersistenceManagerMap);
		this.metaDataCache = metaDataCache;
	}

	public void setMetaDataCache(RedisMetaDataCache metaDataCache)
	{
		this.metaDataCache = metaDataCache;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.datastore.DataStore#getMetaDataCache()
	 */
	@Override
	public MetaDataCache getMetaDataCache()
	{
		return metaDataCache;
	}
}
