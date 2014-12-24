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
import com.flipkart.portkey.common.entity.persistence.ReadConfig;
import com.flipkart.portkey.common.entity.persistence.WriteConfig;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.FailureAction;
import com.flipkart.portkey.common.exception.InvalidAnnotationException;
import com.flipkart.portkey.common.exception.JsonSerializationException;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
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
	private EntityPersistencePreference defaultPersistencePreference;
	private Map<Class<? extends Entity>, EntityPersistencePreference> entityPersistencePreferenceMap;
	private Map<DataStoreType, DataStore> dataStoresMap;
	private ShardIdentifierInterface shardIdentifier = new ShardIdentifier();

	public PersistenceLayer(Map<DataStoreType, DataStore> dataStoresMap,
	        Map<Class<? extends Entity>, EntityPersistencePreference> entityPersistencePreferenceMap)
	{
		this.dataStoresMap = dataStoresMap;
		this.entityPersistencePreferenceMap = entityPersistencePreferenceMap;
	}

	public PersistenceLayer(Map<DataStoreType, DataStore> dataStoresMap,
	        EntityPersistencePreference defaultPersistencePreference)
	{
		this.dataStoresMap = dataStoresMap;
		this.defaultPersistencePreference = defaultPersistencePreference;
	}

	public PersistenceLayer(Map<DataStoreType, DataStore> dataStoresMap,
	        Map<Class<? extends Entity>, EntityPersistencePreference> entityPersistencePreferenceMap,
	        EntityPersistencePreference defaultPersistencePreference)
	{
		this.dataStoresMap = dataStoresMap;
		this.entityPersistencePreferenceMap = entityPersistencePreferenceMap;
		this.defaultPersistencePreference = defaultPersistencePreference;
	}

	/**
	 * @return
	 */
	private EntityPersistencePreference getDefaultPersistencePreference()
	{
		return defaultPersistencePreference;
	}

	/**
	 * @return
	 */
	private ReadConfig getDefaultReadConfig()
	{
		EntityPersistencePreference defaultPersistencePreference = getDefaultPersistencePreference();
		return defaultPersistencePreference.getReadConfig();
	}

	/**
	 * @return
	 */
	private WriteConfig getDefaultWriteConfig()
	{
		EntityPersistencePreference defaultPersistencePreference = getDefaultPersistencePreference();
		return defaultPersistencePreference.getWriteConfig();
	}

	private EntityPersistencePreference getEntityPersistencePreference(Class<? extends Entity> entity)
	{
		if (entityPersistencePreferenceMap == null)
		{
			return getDefaultPersistencePreference();
		}
		return entityPersistencePreferenceMap.get(entity);
	}

	private WriteConfig getWriteConfigForEntity(Class<? extends Entity> clazz)
	{
		EntityPersistencePreference entityPersistencePreference = getEntityPersistencePreference(clazz);
		// TODO: check if following check is really necessary
		if (entityPersistencePreference == null)
		{
			return getDefaultWriteConfig();
		}
		return entityPersistencePreference.getWriteConfig();
	}

	private ReadConfig getReadConfigForEntity(Class<? extends Entity> entity)
	{
		EntityPersistencePreference entityPersistencePreference = getEntityPersistencePreference(entity);
		// TODO: check if following check is really necessary
		if (entityPersistencePreference == null)
		{
			return getDefaultReadConfig();
		}
		return entityPersistencePreference.getReadConfig();
	}

	public void setEntityPersistencePreferenceMap(
	        Map<Class<? extends Entity>, EntityPersistencePreference> entityPersistencePreferenceMap)
	{
		this.entityPersistencePreferenceMap = entityPersistencePreferenceMap;
	}

	private void initializeEntityPersistencePreferenceMap()
	{
		this.entityPersistencePreferenceMap = new HashMap<Class<? extends Entity>, EntityPersistencePreference>();
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

	private DataStore getDataStore(DataStoreType type)
	{
		return dataStoresMap.get(type);
	}

	public void setDataStoresMap(Map<DataStoreType, DataStore> dataStoresMap)
	{
		this.dataStoresMap = dataStoresMap;
	}

	private void initializeDataStoresMap()
	{
		dataStoresMap = new HashMap<DataStoreType, DataStore>();
	}

	public void addDataStore(DataStoreType type, DataStore dataStore)
	{
		if (dataStoresMap == null)
		{
			initializeDataStoresMap();
		}
		dataStoresMap.put(type, dataStore);
	}

	// private Map<String, PersistenceManager> getShardIdToPersistenceManagerMap(DataStoreType type)
	// {
	// DataStore dataStore = getDataStore(type);
	// return dataStore.getShardIdToPersistenceManagerMap();
	// }

	private PersistenceManager getPersistenceManager(DataStoreType type, String shardId)
	{
		DataStore ds = getDataStore(type);
		return ds.getPersistenceManager(shardId);
	}

	private MetaDataCache getMetaDataCache(DataStoreType type)
	{
		DataStore dataStore = getDataStore(type);
		return dataStore.getMetaDataCache();
	}

	private <T extends Entity> int insertIntoDataStore(DataStoreType type, T bean) throws InvalidAnnotationException,
	        ShardNotAvailableException, JsonSerializationException
	{
		MetaDataCache metaDataCache = getMetaDataCache(type);
		String shardKeyFieldName = metaDataCache.getShardKey(bean.getClass());
		Field shardKeyField;
		try
		{
			shardKeyField = bean.getClass().getDeclaredField(shardKeyFieldName);
		}
		catch (NoSuchFieldException e)
		{
			throw new InvalidAnnotationException("Exception while identifying shard key for datastore:" + type
			        + " and entity:" + bean, e);
		}
		catch (SecurityException e)
		{
			throw new InvalidAnnotationException("Exception while identifying shard key for datastore:" + type
			        + " and entity:" + bean, e);
		}
		String shardKey;
		try
		{
			shardKey = shardKeyField.get(bean).toString();
		}
		catch (IllegalArgumentException e)
		{
			throw new InvalidAnnotationException("Exception while getting shard key from entity for datastore:" + type
			        + " and entity:" + bean, e);
		}
		catch (IllegalAccessException e)
		{
			throw new InvalidAnnotationException("Exception while getting shard key from entity for datastore:" + type
			        + " and entity:" + bean, e);
		}
		String shardId = shardIdentifier.getShardId(shardKey);
		PersistenceManager pm = getPersistenceManager(type, shardId);
		int rowsUpdated = pm.insert(bean);
		return rowsUpdated;
	}

	private <T extends Entity> String getShardKey(DataStoreType type, T bean) throws InvalidAnnotationException
	{
		MetaDataCache metaDataCache = getMetaDataCache(type);
		String shardKeyFieldName = metaDataCache.getShardKey(bean.getClass());
		Field shardKeyField;
		try
		{
			shardKeyField = bean.getClass().getDeclaredField(shardKeyFieldName);
		}
		catch (NoSuchFieldException e)
		{
			throw new InvalidAnnotationException("Exception while identifying shard key for datastore:" + type
			        + " and entity " + bean, e);
		}
		catch (SecurityException e)
		{
			throw new InvalidAnnotationException("Exception while identifying shard key for datastore:" + type
			        + " and entity " + bean, e);
		}
		String shardKey;
		try
		{
			shardKey = shardKeyField.get(bean).toString();// TODO: review this
		}
		catch (IllegalArgumentException e)
		{
			throw new InvalidAnnotationException("Exception while getting shard key for datastore:" + type
			        + " and entity " + bean, e);
		}
		catch (IllegalAccessException e)
		{
			throw new InvalidAnnotationException("Exception while getting shard key for datastore:" + type
			        + " and entity " + bean, e);
		}
		return shardKey;
	}

	private <T extends Entity> T generateShardIdAndUpdateBean(DataStoreType type, T bean)
	        throws InvalidAnnotationException
	{
		MetaDataCache metaDataCache = getMetaDataCache(type);
		String shardKeyFieldName = metaDataCache.getShardKey(bean.getClass());
		Field shardKeyField;
		try
		{
			shardKeyField = bean.getClass().getDeclaredField(shardKeyFieldName);
		}
		catch (NoSuchFieldException e)
		{
			throw new InvalidAnnotationException("Exception while identifying shard key for datastore:" + type
			        + " and entity:" + bean, e);
		}
		catch (SecurityException e)
		{
			throw new InvalidAnnotationException("Exception while identifying shard key for datastore:" + type
			        + " and entity:" + bean, e);
		}
		String shardKey;
		try
		{
			shardKey = shardKeyField.get(bean).toString();
		}
		catch (IllegalArgumentException e)
		{
			throw new InvalidAnnotationException("Exception while getting shard key for datastore:" + type
			        + " and entity:" + bean, e);
		}
		catch (IllegalAccessException e)
		{
			throw new InvalidAnnotationException("Exception while getting shard key for datastore:" + type
			        + " and entity:" + bean, e);
		}
		String shardId = shardIdentifier.getShardId(shardKey);
		String newShardKey = shardIdentifier.generateNewShardKey(shardKey, shardId);
		try
		{
			shardKeyField.set(bean, newShardKey);
		}
		catch (IllegalArgumentException e)
		{
			throw new InvalidAnnotationException("Exception while setting new value of shard key for datastore:" + type
			        + " and entity:" + bean, e);
		}
		catch (IllegalAccessException e)
		{
			throw new InvalidAnnotationException("Exception while setting new value of shard key for datastore:" + type
			        + " and entity:" + bean, e);
		}
		return bean;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#insert(com.flipkart.portkey.common.entity.Entity
	 * )
	 */
	public <T extends Entity> Result insert(T bean) throws PortKeyException
	{
		return insert(bean, false);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#insert(com.flipkart.portkey.common.entity.Entity
	 * , boolean)
	 */
	public <T extends Entity> Result insert(T bean, boolean generateShardId) throws InvalidAnnotationException,
	        ShardNotAvailableException, JsonSerializationException
	{
		Result result = new Result();
		WriteConfig writeConfig = getWriteConfigForEntity(bean.getClass());
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		if (generateShardId)
		{
			DataStoreType typeForShardIdGeneration = writeOrder.get(0);
			bean = generateShardIdAndUpdateBean(typeForShardIdGeneration, bean);
		}

		for (DataStoreType type : writeOrder)
		{
			int rowsUpdated = 0;
			try
			{
				rowsUpdated = insertIntoDataStore(type, bean);
			}
			catch (InvalidAnnotationException e)
			{
				FailureAction failureAction = writeConfig.getFailureAction();
				if (failureAction == FailureAction.ABORT)
				{
					throw e;
				}
				continue;
			}
			catch (ShardNotAvailableException e)
			{
				FailureAction failureAction = writeConfig.getFailureAction();
				if (failureAction == FailureAction.ABORT)
				{
					throw e;
				}
				continue;
			}
			catch (JsonSerializationException e)
			{
				FailureAction failureAction = writeConfig.getFailureAction();
				if (failureAction == FailureAction.ABORT)
				{
					throw e;
				}
				continue;
			}
			result.setRowsUpdatedForDataStore(type, rowsUpdated);
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
		WriteConfig writeConfig = getWriteConfigForEntity(bean.getClass());
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType type : writeOrder)
		{
			String shardKey = getShardKey(type, bean);
			String shardId = shardIdentifier.getShardId(shardKey);
			PersistenceManager pm = getPersistenceManager(type, shardId);
			int rowsUpdated = pm.update(bean);
			result.setRowsUpdatedForDataStore(type, rowsUpdated);
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
		WriteConfig writeConfig = getWriteConfigForEntity(clazz);
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType type : writeOrder)
		{
			MetaDataCache metaDataCache = getMetaDataCache(type);
			String shardKeyFieldName = metaDataCache.getShardKey(clazz);
			if (criteria.containsKey(shardKeyFieldName))
			{
				String shardKey = (String) criteria.get(shardKeyFieldName);
				String shardId = shardIdentifier.getShardId(shardKey);
				PersistenceManager pm = getPersistenceManager(type, shardId);
				int rowsUpdated = pm.update(clazz, updateValuesMap, criteria);
				result.setRowsUpdatedForDataStore(type, rowsUpdated);
			}
			else
			{
				List<String> shardIds = dataStoresMap.get(type).getShardIds();
				int rowsUpdated = 0;
				for (String shardId : shardIds)
				{
					PersistenceManager pm = getPersistenceManager(type, shardId);
					rowsUpdated += pm.update(clazz, updateValuesMap, criteria);
				}
				result.setRowsUpdatedForDataStore(type, rowsUpdated);
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
		WriteConfig writeConfig = getWriteConfigForEntity(clazz);
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType type : writeOrder)
		{
			MetaDataCache metaDataCache = getMetaDataCache(type);
			String shardKeyFieldName = metaDataCache.getShardKey(clazz);
			if (criteria.containsKey(shardKeyFieldName))
			{
				String shardKey = (String) criteria.get(shardKeyFieldName);
				String shardId = shardIdentifier.getShardId(shardKey);
				PersistenceManager pm = getPersistenceManager(type, shardId);
				int rowsUpdated = pm.delete(clazz, criteria);
				result.setRowsUpdatedForDataStore(type, rowsUpdated);
			}
			else
			{
				List<String> shardIds = dataStoresMap.get(type).getShardIds();
				int rowsUpdated = 0;
				for (String shardId : shardIds)
				{
					PersistenceManager pm = getPersistenceManager(type, shardId);
					rowsUpdated += pm.delete(clazz, criteria);
				}
				result.setRowsUpdatedForDataStore(type, rowsUpdated);
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
		WriteConfig writeConfig = getWriteConfigForEntity(clazz);
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType type : writeOrder)
		{
			MetaDataCache metaDataCache = getMetaDataCache(type);
			String shardKeyFieldName = metaDataCache.getShardKey(clazz);
			if (criteria.containsKey(shardKeyFieldName))
			{
				String shardKey = (String) criteria.get(shardKeyFieldName);
				String shardId = shardIdentifier.getShardId(shardKey);
				PersistenceManager pm = getPersistenceManager(type, shardId);
				result = pm.getByCriteria(clazz, criteria);
			}
			else
			{
				List<String> shardIds = dataStoresMap.get(type).getShardIds();
				for (String shardId : shardIds)
				{
					PersistenceManager pm = getPersistenceManager(type, shardId);
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
		WriteConfig writeConfig = getWriteConfigForEntity(clazz);
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType type : writeOrder)
		{
			MetaDataCache metaDataCache = getMetaDataCache(type);
			String shardKeyFieldName = metaDataCache.getShardKey(clazz);
			if (criteria.containsKey(shardKeyFieldName))
			{
				String shardKey = (String) criteria.get(shardKeyFieldName);
				String shardId = shardIdentifier.getShardId(shardKey);
				PersistenceManager pm = getPersistenceManager(type, shardId);
				result = pm.getByCriteria(clazz, attributeNames, criteria);
			}
			else
			{
				List<String> shardIds = dataStoresMap.get(type).getShardIds();
				for (String shardId : shardIds)
				{
					PersistenceManager pm = getPersistenceManager(type, shardId);
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
		WriteConfig writeConfig = getWriteConfigForEntity(clazz);
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType type : writeOrder)
		{
			List<String> shardIds = dataStoresMap.get(type).getShardIds();
			for (String shardId : shardIds)
			{
				PersistenceManager pm = getPersistenceManager(type, shardId);
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
		ReadConfig readConfig = getDefaultReadConfig();
		List<DataStoreType> readOrder = readConfig.getReadOrder();
		for (DataStoreType type : readOrder)
		{
			List<String> shardIds = dataStoresMap.get(type).getShardIds();
			for (String shardId : shardIds)
			{
				PersistenceManager pm = getPersistenceManager(type, shardId);
				List<Map<String, Object>> intermediateResult = pm.getBySql(sql, criteria);
				result.addAll(intermediateResult);
			}
			return result;
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getBySql(java.lang.Class, java.util.Map,
	 * java.util.Map)
	 */
	public <T extends Entity> List<T> getBySql(Class<T> clazz, Map<DataStoreType, String> sqlMap,
	        Map<String, Object> criteria) throws PortKeyException
	{
		List<T> result = null;
		ReadConfig readConfig = getReadConfigForEntity(clazz);
		List<DataStoreType> readOrder = readConfig.getReadOrder();
		for (DataStoreType type : readOrder)
		{
			String sql = sqlMap.get(type);
			result = new ArrayList<T>();
			List<String> shardIds = dataStoresMap.get(type).getShardIds();
			for (String shardId : shardIds)
			{
				PersistenceManager pm = getPersistenceManager(type, shardId);
				List<T> intermediateResult = pm.getBySql(clazz, sql, criteria);
				result.addAll(intermediateResult);
			}
			return result;
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getBySql(java.util.Map, java.util.Map)
	 */
	public List<Map<String, Object>> getBySql(Map<DataStoreType, String> sqlMap, Map<String, Object> criteria)
	        throws PortKeyException
	{
		List<Map<String, Object>> result = null;
		ReadConfig readConfig = getDefaultReadConfig();
		List<DataStoreType> readOrder = readConfig.getReadOrder();
		for (DataStoreType type : readOrder)
		{
			String sql = sqlMap.get(type);
			result = new ArrayList<Map<String, Object>>();
			List<String> shardIds = dataStoresMap.get(type).getShardIds();
			for (String shardId : shardIds)
			{
				PersistenceManager pm = getPersistenceManager(type, shardId);
				List<Map<String, Object>> intermediateResult = pm.getBySql(sql, criteria);
				result.addAll(intermediateResult);
			}
			return result;
		}
		return result;
	}

}
