/**
 * 
 */
package com.flipkart.portkey.common.entity.persistence;

/**
 * @author santosh.p
 */
public class EntityPersistencePreference
{

	private EntityPersistenceWriteConfig writeConfig;
	private EntityPersistenceReadConfig readConfig;

	public EntityPersistenceWriteConfig getWriteConfig()
	{
		return writeConfig;
	}

	public void setWriteConfig(EntityPersistenceWriteConfig writeConfig)
	{
		this.writeConfig = writeConfig;
	}

	public EntityPersistenceReadConfig getReadConfig()
	{
		return readConfig;
	}

	public void setReadConfig(EntityPersistenceReadConfig readConfig)
	{
		this.readConfig = readConfig;
	}

}
