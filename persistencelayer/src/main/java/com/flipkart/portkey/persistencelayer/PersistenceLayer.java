/**
 * 
 */
package com.flipkart.portkey.persistencelayer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.datastore.DataStore;
import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.entity.persistence.EntityPersistencePreference;
import com.flipkart.portkey.common.entity.persistence.EntityPersistenceReadConfig;
import com.flipkart.portkey.common.entity.persistence.EntityPersistenceWriteConfig;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.metadata.MetaDataCache;
import com.flipkart.portkey.common.persistence.PersistenceLayerInterface;
import com.flipkart.portkey.common.persistence.PersistenceManager;
import com.flipkart.portkey.common.persistence.Result;
import com.flipkart.portkey.common.sharding.ShardIdentifierInterface;
import com.flipkart.portkey.sharding.ShardIdentifier;

/**
 * @author santosh.p
 */
public class PersistenceLayer implements PersistenceLayerInterface
{
	private List<DataStoreType> defaultReadOrder;
	private List<DataStoreType> defaultWriteOrder;
	private Map<Class<? extends Entity>, EntityPersistencePreference> entityPersistencePreferenceMap;
	private Map<DataStoreType, DataStore> dataStoresMap;
	private Map<DataStoreType, Map<String, PersistenceManager>> dataStoreToPersistenceManagerMap;
	private ShardIdentifierInterface shardIdentifier = new ShardIdentifier();

	public PersistenceLayer()
	{
		entityPersistencePreferenceMap = new HashMap<Class<? extends Entity>, EntityPersistencePreference>();
		dataStoresMap = new HashMap<DataStoreType, DataStore>();
	}

	public PersistenceLayer(Map<Class<? extends Entity>, EntityPersistencePreference> entityPersistencePreferenceMap,
	        Map<DataStoreType, DataStore> dataStoresMap)
	{
		this.entityPersistencePreferenceMap = entityPersistencePreferenceMap;
		this.dataStoresMap = dataStoresMap;
	}

	public Map<Class<? extends Entity>, EntityPersistencePreference> getEntityPersistencePreferenceMap()
	{
		return entityPersistencePreferenceMap;
	}

	public EntityPersistenceWriteConfig getWriteConfigForEntity(Class<? extends Entity> clazz)
	{
		EntityPersistencePreference entityPersistencePreference = getEntityPersistencePreference(clazz);
		if (entityPersistencePreference == null)
		{
			return null;
		}
		return entityPersistencePreference.getWriteConfig();
	}

	public EntityPersistenceReadConfig getReadConfigForEntity(Class<? extends Entity> entity)
	{
		EntityPersistencePreference entityPersistencePreference = getEntityPersistencePreference(entity);
		if (entityPersistencePreference == null)
		{
			return null;
		}
		return entityPersistencePreference.getReadConfig();
	}

	private void initializeEntityPersistencePreferenceMap()
	{
		this.entityPersistencePreferenceMap = new HashMap<Class<? extends Entity>, EntityPersistencePreference>();
	}

	public EntityPersistencePreference getEntityPersistencePreference(Class<? extends Entity> entity)
	{
		if (entityPersistencePreferenceMap == null)
		{
			return null;
		}
		return entityPersistencePreferenceMap.get(entity);
	}

	public void setEntityPersistencePreferenceMap(
	        Map<Class<? extends Entity>, EntityPersistencePreference> entityPersistencePreferenceMap)
	{
		this.entityPersistencePreferenceMap = entityPersistencePreferenceMap;
	}

	public void addToEntityPersistencePreferenceMap(Class<? extends Entity> entity,
	        EntityPersistencePreference entityPersistencePreference)
	{
		if (entityPersistencePreferenceMap == null)
		{
			initializeEntityPersistencePreferenceMap();
		}
		entityPersistencePreferenceMap.put(entity, entityPersistencePreference);
	}

	public Map<DataStoreType, DataStore> getDataStoreConfigMap()
	{
		return dataStoresMap;
	}

	public DataStore getDatastore(DataStoreType ds)
	{
		if (dataStoresMap == null)
		{
			return null;
		}
		return dataStoresMap.get(ds);
	}

	public void setDataStoresMap(Map<DataStoreType, DataStore> dataStoresMap)
	{
		this.dataStoresMap = dataStoresMap;
	}

	public void addDataStore(DataStoreType ds, DataStore dataStore)
	{
		if (dataStoresMap == null)
		{
			dataStoresMap = new HashMap<DataStoreType, DataStore>();
		}
		dataStoresMap.put(ds, dataStore);
	}

	private Map<String, PersistenceManager> getIdToPersistenceManagerMap(DataStoreType ds)
	{
		// TODO check if map is not initialized
		return dataStoreToPersistenceManagerMap.get(ds);
	}

	private PersistenceManager getPersistenceManager(DataStoreType ds, String shardId)
	{
		Map<String, PersistenceManager> idToPersistenceManagerMap = getIdToPersistenceManagerMap(ds);
		return idToPersistenceManagerMap.get(shardId);
	}

	private MetaDataCache getMetaDataCache(DataStoreType ds)
	{
		return getDatastore(ds).getMetaDataCache();
	}

	public void setDefaultReadOrder(List<DataStoreType> defaultReadOrder)
	{
		this.defaultReadOrder = defaultReadOrder;
	}

	public List<DataStoreType> getDefaultReadOrder()
	{
		return defaultReadOrder;
	}

	public void setDefaultWriteOrder(List<DataStoreType> defaultWriteOrder)
	{
		this.defaultWriteOrder = defaultWriteOrder;
	}

	public List<DataStoreType> getDefaultWriteOrder()
	{
		return defaultWriteOrder;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#insert(com.flipkart.portkey.common.entity.Entity
	 * )
	 */
	public <T extends Entity> Result insert(T bean) throws PortKeyException
	{
		Result result = new Result();
		EntityPersistenceWriteConfig writeConfig = getWriteConfigForEntity(bean.getClass());
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType ds : writeOrder)
		{
			MetaDataCache metaDataCache = getMetaDataCache(ds);
			String shardKeyFieldName = metaDataCache.getShardKey(bean.getClass());
			Field shardKeyField;
			try
			{
				shardKeyField = bean.getClass().getDeclaredField(shardKeyFieldName);
			}
			catch (NoSuchFieldException e)
			{
				throw new PortKeyException("Exception while identifying shard key for datastore:" + ds + " and entity "
				        + bean, e);
			}
			catch (SecurityException e)
			{
				throw new PortKeyException("Exception while identifying shard key for datastore:" + ds + " and entity "
				        + bean, e);
			}
			String shardKey;
			try
			{
				shardKey = shardKeyField.get(bean).toString();
			}
			catch (IllegalArgumentException e)
			{
				throw new PortKeyException("Exception while getting shard key for datastore:" + ds + " and entity "
				        + bean, e);
			}
			catch (IllegalAccessException e)
			{
				throw new PortKeyException("Exception while getting shard key for datastore:" + ds + " and entity "
				        + bean, e);
			}
			String shardId = shardIdentifier.getShardId(shardKey);
			PersistenceManager pm = getPersistenceManager(ds, shardId);
			int rowsUpdated = pm.insert(bean);
			result.setRowsUpdatedForDataStore(ds, rowsUpdated);
		}
		result.setEntity(bean);
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#insert(com.flipkart.portkey.common.entity.Entity
	 * , boolean)
	 */
	public <T extends Entity> Result insert(T bean, boolean generateShardId) throws PortKeyException
	{
		Result result = new Result();
		EntityPersistenceWriteConfig writeConfig = getWriteConfigForEntity(bean.getClass());
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		boolean shardKeyGenerated = false;
		for (DataStoreType ds : writeOrder)
		{
			MetaDataCache metaDataCache = getMetaDataCache(ds);
			String shardKeyFieldName = metaDataCache.getShardKey(bean.getClass());
			Field shardKeyField;
			try
			{
				shardKeyField = bean.getClass().getDeclaredField(shardKeyFieldName);
			}
			catch (NoSuchFieldException e)
			{
				throw new PortKeyException("Exception while identifying shard key for datastore:" + ds + " and entity "
				        + bean, e);
			}
			catch (SecurityException e)
			{
				throw new PortKeyException("Exception while identifying shard key for datastore:" + ds + " and entity "
				        + bean, e);
			}
			String shardKey;
			try
			{
				shardKey = shardKeyField.get(bean).toString();
			}
			catch (IllegalArgumentException e)
			{
				throw new PortKeyException("Exception while getting shard key for datastore:" + ds + " and entity "
				        + bean, e);
			}
			catch (IllegalAccessException e)
			{
				throw new PortKeyException("Exception while getting shard key for datastore:" + ds + " and entity "
				        + bean, e);
			}
			String shardId = shardIdentifier.getShardId(shardKey);
			if (!shardKeyGenerated)
			{
				String newShardKey = shardIdentifier.generateNewShardKey(shardKey, shardId);
				try
				{
					shardKeyField.set(bean, newShardKey);
				}
				catch (IllegalArgumentException e)
				{
					throw new PortKeyException("Exception while setting new value of shard key for datastore:" + ds
					        + " and entity " + bean, e);
				}
				catch (IllegalAccessException e)
				{
					throw new PortKeyException("Exception while setting new value of shard key for datastore:" + ds
					        + " and entity " + bean, e);
				}
			}
			PersistenceManager pm = getPersistenceManager(ds, shardId);
			int rowsUpdated = pm.insert(bean);
			result.setRowsUpdatedForDataStore(ds, rowsUpdated);
		}
		result.setEntity(bean);
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#update(com.flipkart.portkey.common.entity.Entity
	 * )
	 */
	public <T extends Entity> Result update(T bean) throws PortKeyException
	{
		Result result = new Result();
		EntityPersistenceWriteConfig writeConfig = getWriteConfigForEntity(bean.getClass());
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType ds : writeOrder)
		{
			MetaDataCache metaDataCache = getMetaDataCache(ds);
			String shardKeyFieldName = metaDataCache.getShardKey(bean.getClass());
			Field shardKeyField;
			try
			{
				shardKeyField = bean.getClass().getDeclaredField(shardKeyFieldName);
			}
			catch (NoSuchFieldException e)
			{
				throw new PortKeyException("Exception while identifying shard key for datastore:" + ds + " and entity "
				        + bean, e);
			}
			catch (SecurityException e)
			{
				throw new PortKeyException("Exception while identifying shard key for datastore:" + ds + " and entity "
				        + bean, e);
			}
			String shardKey;
			try
			{
				shardKey = shardKeyField.get(bean).toString();
			}
			catch (IllegalArgumentException e)
			{
				throw new PortKeyException("Exception while getting shard key for datastore:" + ds + " and entity "
				        + bean, e);
			}
			catch (IllegalAccessException e)
			{
				throw new PortKeyException("Exception while getting shard key for datastore:" + ds + " and entity "
				        + bean, e);
			}
			String shardId = shardIdentifier.getShardId(shardKey);
			PersistenceManager pm = getPersistenceManager(ds, shardId);
			int rowsUpdated = pm.update(bean);
			result.setRowsUpdatedForDataStore(ds, rowsUpdated);
		}
		result.setEntity(bean);
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#update(java.lang.Class, java.util.Map,
	 * java.util.Map)
	 */
	public <T extends Entity> Result update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws PortKeyException
	{
		Result result = new Result();
		EntityPersistenceWriteConfig writeConfig = getWriteConfigForEntity(clazz);
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType ds : writeOrder)
		{
			MetaDataCache metaDataCache = getMetaDataCache(ds);
			String shardKeyFieldName = metaDataCache.getShardKey(clazz);
			if (criteria.containsKey(shardKeyFieldName))
			{
				String shardKey = (String) criteria.get(shardKeyFieldName);
				String shardId = shardIdentifier.getShardId(shardKey);
				PersistenceManager pm = getPersistenceManager(ds, shardId);
				int rowsUpdated = pm.update(clazz, updateValuesMap, criteria);
				result.setRowsUpdatedForDataStore(ds, rowsUpdated);
			}
			else
			{
				List<PersistenceManager> persistenceManagers = dataStoresMap.get(ds).getPersistenceManagers();
				int rowsUpdated = 0;
				for (PersistenceManager pm : persistenceManagers)
				{
					rowsUpdated += pm.update(clazz, updateValuesMap, criteria);
				}
				result.setRowsUpdatedForDataStore(ds, rowsUpdated);
			}
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#delete(java.lang.Class, java.util.Map)
	 */
	public <T extends Entity> Result delete(Class<T> clazz, Map<String, Object> criteria) throws PortKeyException
	{
		Result result = new Result();
		EntityPersistenceWriteConfig writeConfig = getWriteConfigForEntity(clazz);
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType ds : writeOrder)
		{
			MetaDataCache metaDataCache = getMetaDataCache(ds);
			String shardKeyFieldName = metaDataCache.getShardKey(clazz);
			if (criteria.containsKey(shardKeyFieldName))
			{
				String shardKey = (String) criteria.get(shardKeyFieldName);
				String shardId = shardIdentifier.getShardId(shardKey);
				PersistenceManager pm = getPersistenceManager(ds, shardId);
				int rowsUpdated = pm.delete(clazz, criteria);
				result.setRowsUpdatedForDataStore(ds, rowsUpdated);
			}
			else
			{
				List<PersistenceManager> persistenceManagers = dataStoresMap.get(ds).getPersistenceManagers();
				int rowsUpdated = 0;
				for (PersistenceManager pm : persistenceManagers)
				{
					rowsUpdated += pm.delete(clazz, criteria);
				}
				result.setRowsUpdatedForDataStore(ds, rowsUpdated);
			}
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getByCriteria(java.lang.Class,
	 * java.util.Map)
	 */
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws PortKeyException
	{
		List<T> result = new ArrayList<T>();
		EntityPersistenceWriteConfig writeConfig = getWriteConfigForEntity(clazz);
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType ds : writeOrder)
		{
			MetaDataCache metaDataCache = getMetaDataCache(ds);
			String shardKeyFieldName = metaDataCache.getShardKey(clazz);
			if (criteria.containsKey(shardKeyFieldName))
			{
				String shardKey = (String) criteria.get(shardKeyFieldName);
				String shardId = shardIdentifier.getShardId(shardKey);
				PersistenceManager pm = getPersistenceManager(ds, shardId);
				result = pm.getByCriteria(clazz, criteria);
			}
			else
			{
				List<PersistenceManager> persistenceManagers = dataStoresMap.get(ds).getPersistenceManagers();
				for (PersistenceManager pm : persistenceManagers)
				{
					List<T> intermediateResult = pm.getByCriteria(clazz, criteria);
					result.addAll(intermediateResult);
				}
			}
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getByCriteria(java.lang.Class,
	 * java.util.List, java.util.Map)
	 */
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws PortKeyException
	{
		List<T> result = new ArrayList<T>();
		EntityPersistenceWriteConfig writeConfig = getWriteConfigForEntity(clazz);
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType ds : writeOrder)
		{
			MetaDataCache metaDataCache = getMetaDataCache(ds);
			String shardKeyFieldName = metaDataCache.getShardKey(clazz);
			if (criteria.containsKey(shardKeyFieldName))
			{
				String shardKey = (String) criteria.get(shardKeyFieldName);
				String shardId = shardIdentifier.getShardId(shardKey);
				PersistenceManager pm = getPersistenceManager(ds, shardId);
				result = pm.getByCriteria(clazz, attributeNames, criteria);
			}
			else
			{
				List<PersistenceManager> persistenceManagers = dataStoresMap.get(ds).getPersistenceManagers();
				for (PersistenceManager pm : persistenceManagers)
				{
					List<T> intermediateResult = pm.getByCriteria(clazz, attributeNames, criteria);
					result.addAll(intermediateResult);
				}
			}
			return result;
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getBySql(java.lang.Class,
	 * java.lang.String, java.util.Map)
	 */
	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws PortKeyException
	{
		List<T> result = new ArrayList<T>();
		EntityPersistenceWriteConfig writeConfig = getWriteConfigForEntity(clazz);
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType ds : writeOrder)
		{
			List<PersistenceManager> persistenceManagers = dataStoresMap.get(ds).getPersistenceManagers();
			for (PersistenceManager pm : persistenceManagers)
			{
				List<T> intermediateResult = pm.getBySql(clazz, sql, criteria);
				result.addAll(intermediateResult);
			}
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getBySql(java.lang.String, java.util.Map)
	 */
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria) throws PortKeyException
	{
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		List<DataStoreType> readOrder = getDefaultReadOrder();
		for (DataStoreType ds : readOrder)
		{
			List<PersistenceManager> persistenceManagers = dataStoresMap.get(ds).getPersistenceManagers();
			for (PersistenceManager pm : persistenceManagers)
			{
				List<Map<String, Object>> intermediateResult = pm.getBySql(sql, criteria);
				result.addAll(intermediateResult);
			}
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getBySql(java.lang.Class, java.util.Map,
	 * java.util.Map)
	 */
	public <T extends Entity> List<Entity> getBySql(Class<T> clazz, Map<DataStoreType, String> sqlMap,
	        Map<String, Object> criteria)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getBySql(java.util.Map, java.util.Map)
	 */
	public List<Map<String, Object>> getBySql(Map<DataStoreType, String> sqlMap, Map<String, Object> criteria)
	{
		// TODO Auto-generated method stub
		return null;
	}

}
