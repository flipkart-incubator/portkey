/**
 * 
 */
package com.flipkart.portkey.common.persistence.config;

import java.util.HashMap;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.entity.persistence.EntityPersistencePreference;

/**
 * @author santosh.p
 */
public class PersistenceConfig
{
	private Map<Class<? extends Entity>, EntityPersistencePreference> persistenceConfig;

	public PersistenceConfig()
	{
		this.persistenceConfig = new HashMap<Class<? extends Entity>, EntityPersistencePreference>();
	}

	public Map<Class<? extends Entity>, EntityPersistencePreference> getPersistenceConfig()
	{
		return persistenceConfig;
	}

	public void setPersistenceConfig(Map<Class<? extends Entity>, EntityPersistencePreference> persistenceConfig)
	{
		this.persistenceConfig = persistenceConfig;
	}

	public void addToPersistenceonfig(Class<? extends Entity> clazz, EntityPersistencePreference persistencePerference)
	{
		if (this.persistenceConfig == null)
		{
			this.persistenceConfig = new HashMap<Class<? extends Entity>, EntityPersistencePreference>();
		}
		this.persistenceConfig.put(clazz, persistencePerference);
	}
}
