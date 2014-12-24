/**
 * 
 */
package com.flipkart.portkey.rdbms.datastore;

import java.util.Map;

import com.flipkart.portkey.common.datastore.AbstractDataStore;
import com.flipkart.portkey.common.persistence.PersistenceManager;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;

/**
 * @author santosh.p
 */
public class RdbmsDataStore extends AbstractDataStore
{

	private RdbmsMetaDataCache metaDataCache;

	public RdbmsDataStore(Map<String, PersistenceManager> shardIdToPersistenceManagerMap,
	        RdbmsMetaDataCache metaDataCache)
	{
		super(shardIdToPersistenceManagerMap);
		this.metaDataCache = metaDataCache;
	}

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
}
