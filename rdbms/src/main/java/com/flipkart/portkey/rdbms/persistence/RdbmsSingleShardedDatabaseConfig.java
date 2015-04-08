package com.flipkart.portkey.rdbms.persistence;

import java.util.ArrayList;
import java.util.List;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.sharding.ShardIdentifier;
import com.flipkart.portkey.rdbms.sharding.RdbmsShardIdentifierForSingleShard;

public class RdbmsSingleShardedDatabaseConfig implements RdbmsDatabaseConfig
{
	RdbmsPersistenceManager persistenceManager;
	ShardIdentifier shardIdentifier = new RdbmsShardIdentifierForSingleShard();

	@Override
	public <T extends Entity> RdbmsPersistenceManager getPersistenceManager(T bean)
	{
		return persistenceManager;
	}

	@Override
	public <T extends Entity> RdbmsPersistenceManager getPersistenceManager(String shardKey)
	{
		return persistenceManager;
	}

	@Override
	public <T extends Entity> List<RdbmsPersistenceManager> getAllPersistenceManagers()
	{
		List<RdbmsPersistenceManager> allPersitenceManagers = new ArrayList<RdbmsPersistenceManager>();
		allPersitenceManagers.add(persistenceManager);
		return allPersitenceManagers;
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

	@Override
	public <T extends Entity> T generateShardIdAndUpdateBean(T bean)
	{
		return bean;
	}
}
