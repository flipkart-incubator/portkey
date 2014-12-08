/**
 * 
 */
package com.flipkart.portkey.common.persistence.config;

import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.entity.persistence.EntityPersistencePreference;

/**
 * @author santosh.p
 */
public class PersistenceConfig
{
	private Map<Entity, EntityPersistencePreference> persistenceConfig;

	public Map<Entity, EntityPersistencePreference> getPersistenceConfig()
	{
		return persistenceConfig;
	}

	public void setPersistenceConfig(Map<Entity, EntityPersistencePreference> persistenceConfig)
	{
		this.persistenceConfig = persistenceConfig;
	}
}
