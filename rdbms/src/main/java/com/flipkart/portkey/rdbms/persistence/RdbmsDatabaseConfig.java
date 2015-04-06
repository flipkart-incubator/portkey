package com.flipkart.portkey.rdbms.persistence;

import com.flipkart.portkey.common.sharding.ShardIdentifier;

public interface RdbmsDatabaseConfig
{
	public RdbmsPersistenceManager getPersistenceManager(String key);

	public ShardIdentifier getShardIdentifier();
}
