/**
 * 
 */
package com.flipkart.portkey.common.datastore;

import java.util.List;

import com.flipkart.portkey.common.metadata.MetaDataCache;
import com.flipkart.portkey.common.persistence.PersistenceManager;

/**
 * An instance of DataStore represents a data store repository. e.g MySQL, Redis. One DataStore instance contains one or
 * more shards of same data store type.
 * @author santosh.p
 */
public interface DataStore
{

	public PersistenceManager getPersistenceManager(String shardId);

	public List<String> getShardIds();

	public MetaDataCache getMetaDataCache();
}
