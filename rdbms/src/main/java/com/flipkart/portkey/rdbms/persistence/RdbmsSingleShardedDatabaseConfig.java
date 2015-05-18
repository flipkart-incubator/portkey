package com.flipkart.portkey.rdbms.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.common.sharding.ShardIdentifier;
import com.flipkart.portkey.rdbms.persistence.config.RdbmsConnectionConfig;
import com.flipkart.portkey.rdbms.sharding.RdbmsShardIdentifierForSingleShard;
import com.flipkart.portkey.rdbms.transaction.RdbmsTransactionManager;

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

	@Override
	public <T extends Entity> RdbmsTransactionManager getTransactionManager(T bean) throws ShardNotAvailableException
	{
		return persistenceManager.getTransactionManager();
	}
}
