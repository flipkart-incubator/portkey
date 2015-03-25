/**
 * 
 */
package com.flipkart.portkey.common.entity.persistence;

/**
 * The class stores persistence configurations for Entities. Configuration includes the list of data stores to and from
 * which the entity is written into and read from. It also stores the order in which data stores should be hit for
 * reading and writing Entities.
 * @author santosh.p
 */
public class EntityPersistencePreference
{

	private WriteConfig writeConfig;
	private ReadConfig readConfig;

	public WriteConfig getWriteConfig()
	{
		return writeConfig;
	}

	public void setWriteConfig(WriteConfig writeConfig)
	{
		this.writeConfig = writeConfig;
	}

	public ReadConfig getReadConfig()
	{
		return readConfig;
	}

	public void setReadConfig(ReadConfig readConfig)
	{
		this.readConfig = readConfig;
	}

}
