package com.flipkart.portkey.rdbms.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.sharding.ShardIdentifier;
import com.flipkart.portkey.rdbms.persistence.config.RdbmsConnectionConfig;
import com.flipkart.portkey.rdbms.sharding.RdbmsShardIdentifierForSingleShard;

public class RdbmsSingleShardedDatabaseConfig implements RdbmsDatabaseConfig
{
	RdbmsPersistenceManager persistenceManager;
	ShardIdentifier shardIdentifier = new RdbmsShardIdentifierForSingleShard();

	public RdbmsSingleShardedDatabaseConfig(RdbmsConnectionConfig connectionConfig)
	{
		this.persistenceManager = new RdbmsPersistenceManager(connectionConfig);
	}

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

	@Override
	public Map<String, ShardStatus> healthCheck()
	{
		return new HashMap<String, ShardStatus>();
	}
}
