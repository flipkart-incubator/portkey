/**
 * 
 */
package com.flipkart.portkey.common.datastore;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.metadata.MetaDataCache;
import com.flipkart.portkey.common.persistence.PersistenceManager;

/**
 * @author santosh.p
 */
public interface DataStore
{
	public PersistenceManager getPersistenceManager(String id);

	public Map<String, PersistenceManager> getIdToPersistenceManagerMap();

	public List<PersistenceManager> getPersistenceManagers();

	public MetaDataCache getMetaDataCache();
}
