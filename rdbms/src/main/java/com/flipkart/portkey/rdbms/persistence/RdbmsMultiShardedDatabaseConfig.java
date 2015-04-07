package com.flipkart.portkey.rdbms.persistence;

import java.util.Map;

import com.flipkart.portkey.common.sharding.ShardIdentifier;
import com.flipkart.portkey.rdbms.sharding.RdbmsShardIdentifier;

public class RdbmsMultiShardedDatabaseConfig implements RdbmsDatabaseConfig
{
	Map<String, RdbmsPersistenceManager> shardIdToPersistenceManagerMap;
	ShardIdentifier shardIdentifler = new RdbmsShardIdentifier();

	@Override
	public RdbmsPersistenceManager getPersistenceManager(String shardId)
	{
		return shardIdToPersistenceManagerMap.get(shardId);
	}

	public void setShardIdToPersistenceManagerMap(Map<String, RdbmsPersistenceManager> shardIdToPersistenceManagerMap)
	{
		this.shardIdToPersistenceManagerMap = shardIdToPersistenceManagerMap;
	}

	public void addToShardIdToPersistenceManagerMap(String shardId, RdbmsPersistenceManager persistenceManager)
	{
		this.shardIdToPersistenceManagerMap.put(shardId, persistenceManager);
	}

	@Override
	public ShardIdentifier getShardIdentifier()
	{
		return shardIdentifler;
	}
}
