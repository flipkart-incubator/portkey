package com.flipkart.portkey.rdbms.persistence;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.common.sharding.ShardIdentifier;

public interface RdbmsDatabaseConfig
{
	public <T extends Entity> RdbmsPersistenceManager getPersistenceManager(T bean) throws ShardNotAvailableException;

	public <T extends Entity> RdbmsPersistenceManager getPersistenceManager(String shardKey);

	public <T extends Entity> List<RdbmsPersistenceManager> getAllPersistenceManagers();

	public ShardIdentifier getShardIdentifier();

	public <T extends Entity> T generateShardIdAndUpdateBean(T bean) throws ShardNotAvailableException;

	public Map<String, ShardStatus> healthCheck();

	public <T extends Entity> RdbmsTransactionManager getTransactionManager(T bean) throws ShardNotAvailableException;
}
