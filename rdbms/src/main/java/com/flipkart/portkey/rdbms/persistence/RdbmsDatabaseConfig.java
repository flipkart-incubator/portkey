package com.flipkart.portkey.rdbms.persistence;


public abstract class RdbmsDatabaseConfig
{
	private final boolean isSharded;

	RdbmsDatabaseConfig(boolean isSharded)
	{
		this.isSharded = isSharded;
	}

	public boolean isSharded()
	{
		return isSharded;
	}
}
