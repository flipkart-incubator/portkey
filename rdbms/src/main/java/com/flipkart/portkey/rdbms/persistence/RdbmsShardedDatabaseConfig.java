package com.flipkart.portkey.rdbms.persistence;

import java.util.Map;

import com.flipkart.portkey.common.sharding.ShardIdentifier;
import com.flipkart.portkey.rdbms.sharding.RdbmsShardIdentifier;

public class RdbmsShardedDatabaseConfig extends RdbmsDatabaseConfig
{
	ShardIdentifier shardIdentifier = new RdbmsShardIdentifier();
	Map<String, RdbmsPersistenceManager> shardIdToPersistenceManagerMap;

	public RdbmsShardedDatabaseConfig()
	{
		super(true);
	}

	public ShardIdentifier getShardIdentifier()
	{
		return shardIdentifier;
	}

	public void setShardIdentifier(ShardIdentifier shardIdentifier)
	{
		this.shardIdentifier = shardIdentifier;
	}

	public Map<String, RdbmsPersistenceManager> getShardIdToPersistenceManagerMap()
	{
		return shardIdToPersistenceManagerMap;
	}

	public RdbmsPersistenceManager getPersistenceManagerFromShardId(String shardId)
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
}
