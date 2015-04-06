package com.flipkart.portkey.rdbms.persistence;

public class RdbmsNonShardedDatabaseConfig extends RdbmsDatabaseConfig
{
	RdbmsNonShardedDatabaseConfig()
	{
		super(false);
	}

	RdbmsPersistenceManager persistenceManager;

	public RdbmsPersistenceManager getPersistenceManager()
	{
		return persistenceManager;
	}

	public void setPersistenceManager(RdbmsPersistenceManager persistenceManager)
	{
		this.persistenceManager = persistenceManager;
	}
}
