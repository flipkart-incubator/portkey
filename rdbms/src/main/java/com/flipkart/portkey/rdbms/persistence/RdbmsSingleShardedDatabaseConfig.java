package com.flipkart.portkey.rdbms.persistence;

import com.flipkart.portkey.common.sharding.ShardIdentifier;
import com.flipkart.portkey.rdbms.sharding.RdbmsShardIdentifierForSingleShard;

public class RdbmsSingleShardedDatabaseConfig implements RdbmsDatabaseConfig
{
	RdbmsPersistenceManager persistenceManager;
	ShardIdentifier shardIdentifier = new RdbmsShardIdentifierForSingleShard();

	@Override
	public RdbmsPersistenceManager getPersistenceManager(String key)
	{
		return persistenceManager;
	}

	public void setPersistenceManager(RdbmsPersistenceManager persistenceManager)
	{
		this.persistenceManager = persistenceManager;
	}

	@Override
	public ShardIdentifier getShardIdentifier()
	{
		return shardIdentifier;
	}

	public void setShardIdentifier(ShardIdentifier shardIdentifier)
	{
		this.shardIdentifier = shardIdentifier;
	}
}
