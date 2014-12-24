/**
 * 
 */
package com.flipkart.portkey.common.datastore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.persistence.PersistenceManager;

/**
 * @author santosh.p
 */
public abstract class AbstractDataStore implements DataStore
{

	private Map<String, PersistenceManager> shardIdToPersistenceManagerMap;

	/**
     * 
     */
	protected AbstractDataStore(Map<String, PersistenceManager> shardIdToPersistenceManagerMap)
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
