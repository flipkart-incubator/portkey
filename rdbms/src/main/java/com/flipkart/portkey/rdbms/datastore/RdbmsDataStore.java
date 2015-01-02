/**
 * 
 */
package com.flipkart.portkey.rdbms.datastore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.datastore.DataStoreConfig;
import com.flipkart.portkey.common.persistence.PersistenceManager;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;

/**
 * @author santosh.p
 */
public class RdbmsDataStore implements DataStoreConfig
{

	private Map<String, PersistenceManager> shardIdToPersistenceManagerMap;
	private RdbmsMetaDataCache metaDataCache;

	public void setMetaDataCache(RdbmsMetaDataCache metaDataCache)
	{
		this.metaDataCache = metaDataCache;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.datastore.DataStore#getMetaDataCache(com.flipkart.portkey.common.entity.Entity)
	 */
	public RdbmsMetaDataCache getMetaDataCache()
	{
		return metaDataCache;
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

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.datastore.DataStore#getShardIds()
	 */
	public List<String> getShardIds()
	{
		return new ArrayList<String>(shardIdToPersistenceManagerMap.keySet());
	}
}
