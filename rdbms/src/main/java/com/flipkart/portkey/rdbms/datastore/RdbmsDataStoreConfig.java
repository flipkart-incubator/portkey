/**
 * 
 */
package com.flipkart.portkey.rdbms.datastore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.flipkart.portkey.common.datastore.DataStoreConfig;
import com.flipkart.portkey.common.persistence.PersistenceManager;
import com.flipkart.portkey.common.sharding.ShardIdentifier;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;
import com.flipkart.portkey.rdbms.sharding.RdbmsShardIdentifierForSingleShard;

/**
 * @author santosh.p
 */
public class RdbmsDataStoreConfig implements DataStoreConfig, InitializingBean
{
	private static final Logger logger = Logger.getLogger(RdbmsDataStoreConfig.class);
	private Map<String, PersistenceManager> shardIdToPersistenceManagerMap;
	private RdbmsMetaDataCache metaDataCache;
	private ShardIdentifier shardIdentifier;

	public void setMetaDataCache(RdbmsMetaDataCache metaDataCache)
	{
		this.metaDataCache = metaDataCache;
	}

	/*
	 * (non-Javadoc
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

	public ShardIdentifier getShardIdentifier()
	{
		return shardIdentifier;
	}

	public void setShardIdentifier(ShardIdentifier shardIdentifier)
	{
		this.shardIdentifier = shardIdentifier;
	}

	@Override
	public void afterPropertiesSet() throws Exception
	{
		Assert.notNull(shardIdToPersistenceManagerMap);
		Assert.notNull(metaDataCache);
		if (shardIdentifier == null)
		{
			logger.warn("No shard identifier provided, using RdbmsShardIdentifierForSingleShard");
			shardIdentifier = new RdbmsShardIdentifierForSingleShard();
		}
		logger.info("Successfully initialized RdbmsDataStore");
	}
}
