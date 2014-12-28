/**
 * 
 */
package com.flipkart.portkey.persistencelayer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.flipkart.portkey.common.datastore.DataStore;
import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.entity.persistence.EntityPersistencePreference;
import com.flipkart.portkey.common.entity.persistence.ReadConfig;
import com.flipkart.portkey.common.entity.persistence.WriteConfig;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.FailureAction;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.InvalidAnnotationException;
import com.flipkart.portkey.common.exception.JsonSerializationException;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.common.metadata.MetaDataCache;
import com.flipkart.portkey.common.persistence.PersistenceLayerInterface;
import com.flipkart.portkey.common.persistence.PersistenceManager;
import com.flipkart.portkey.common.persistence.Result;
import com.flipkart.portkey.common.sharding.ShardIdentifierInterface;
import com.flipkart.portkey.common.sharding.ShardLifeCycleManagerInterface;
import com.flipkart.portkey.sharding.ShardIdentifier;
import com.flipkart.portkey.sharding.ShardLifeCycleManager;

/**
 * @author santosh.p
 */
public class PersistenceLayer implements PersistenceLayerInterface, InitializingBean
{
	private static final Logger logger = Logger.getLogger(PersistenceLayer.class);
	private EntityPersistencePreference defaultPersistencePreference;
	private Map<Class<? extends Entity>, EntityPersistencePreference> entityPersistencePreferenceMap;
	private Map<DataStoreType, DataStore> dataStoresMap;
	private ShardIdentifierInterface shardIdentifier;
	private ShardLifeCycleManagerInterface shardLifeCycleManager;

	class HealthCheckScheduler implements Runnable
	{
		@Override
		public void run()
		{
			logger.info("running healthckecker");
			// TODO: try fetching a snapshot of shard statuses, instead of individual fetches from shardLifeCycleManager
			for (DataStoreType type : dataStoresMap.keySet())
			{
				DataStore ds = dataStoresMap.get(type);
				List<String> shardIds = ds.getShardIds();
				for (String shardId : shardIds)
				{
					PersistenceManager pm = ds.getPersistenceManager(shardId);
					ShardStatus currentStatus = pm.healthCheck();
					ShardStatus previousStatus = shardLifeCycleManager.getShardStatus(type, shardId);
					logger.info("datastoretype=" + type + " shardId=" + shardId + " current status=" + currentStatus
					        + " previous status=" + previousStatus);
					if (!previousStatus.equals(currentStatus))
					{
						shardLifeCycleManager.setShardStatus(type, shardId, currentStatus);
					}
				}
			}
			logger.info("healthcheck complete");
		}
	}

	public void setDefaultPersistencePreference(EntityPersistencePreference defaultPersistencePreference)
	{
		this.defaultPersistencePreference = defaultPersistencePreference;
	}

	public void setEntityPersistencePreferenceMap(
	        Map<Class<? extends Entity>, EntityPersistencePreference> entityPersistencePreferenceMap)
	{
		this.entityPersistencePreferenceMap = entityPersistencePreferenceMap;
	}

	public void setDataStoresMap(Map<DataStoreType, DataStore> dataStoresMap)
	{
		this.dataStoresMap = dataStoresMap;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception
	{
		logger.info("Entry: afterPropertiesSet");
		Assert.notNull(defaultPersistencePreference);
		Assert.notNull(dataStoresMap);

		logger.info("Assertions passed");

		shardIdentifier = new ShardIdentifier();
		logger.info("shardIdentifier initialized");

		logger.info("initializing ShardLifeCycleManager");
		List<DataStoreType> dataStoreTypes = new ArrayList<DataStoreType>(dataStoresMap.keySet());
		shardLifeCycleManager = new ShardLifeCycleManager(dataStoreTypes);
		logger.info("number of data stores to be registered=" + dataStoreTypes.size());
		logger.info("datastores=" + dataStoreTypes);
		for (DataStoreType type : dataStoreTypes)
		{
			DataStore ds = dataStoresMap.get(type);
			List<String> shardIds = ds.getShardIds();
			logger.info("registering shards for datastoretype=" + dataStoreTypes);
			for (String shardId : shardIds)
			{
				logger.info("registering shard id=" + shardId);
				shardLifeCycleManager.setShardStatus(type, shardId, ShardStatus.getDefaultStatus());
			}
		}
		logger.info("ShardLifeCycleManager initialized");
		ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(5);
		HealthCheckScheduler healthCheckScheduler = new HealthCheckScheduler();
		scheduledThreadPool.scheduleAtFixedRate(healthCheckScheduler, 0, 100, TimeUnit.SECONDS);
		Thread.sleep(3000);
		logger.info("scheduled health checker");
		logger.info("Initialized Persistence Layer");
	}

	private EntityPersistencePreference getDefaultPersistencePreference()
	{
		return defaultPersistencePreference;
	}

	private ReadConfig getDefaultReadConfig()
	{
		EntityPersistencePreference defaultPersistencePreference = getDefaultPersistencePreference();
		return defaultPersistencePreference.getReadConfig();
	}

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
		if (entityPersistencePreference == null)
		{
			return getDefaultWriteConfig();
		}
		return entityPersistencePreference.getWriteConfig();
	}

	private ReadConfig getReadConfigForEntity(Class<? extends Entity> entity)
	{
		EntityPersistencePreference entityPersistencePreference = getEntityPersistencePreference(entity);
		if (entityPersistencePreference == null)
		{
			return getDefaultReadConfig();
		}
		return entityPersistencePreference.getReadConfig();
	}

	private DataStore getDataStore(DataStoreType type)
	{
		return dataStoresMap.get(type);
	}

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

	private <T extends Entity> Field getFieldFromBean(T bean, String fieldName) throws InvalidAnnotationException
	{
		Field field = null;
		try
		{

			field = bean.getClass().getDeclaredField(fieldName);
			field.setAccessible(true);
		}
		catch (NoSuchFieldException e)
		{
			logger.info("exception while trying to get field from field name, fieldName=" + fieldName + "\n bean="
			        + bean + "\n" + e);
			throw new InvalidAnnotationException("bean=" + bean + "\nfieldName=" + fieldName, e);
		}
		catch (SecurityException e)
		{
			logger.info("exception while trying to get field from field name, fieldName=" + fieldName + "\n bean="
			        + bean + "\n" + e);
			throw new InvalidAnnotationException("bean=" + bean + "\nfieldName=" + fieldName, e);
		}
		return field;
	}

	private <T extends Entity> String getFieldValueFromBean(T bean, String fieldName) throws InvalidAnnotationException
	{
		String value = null;
		Field field = null;
		field = getFieldFromBean(bean, fieldName);
		try
		{
			if (!field.isAccessible())
			{
				field.setAccessible(true);
			}
			value = field.get(bean).toString();
		}
		catch (IllegalArgumentException e)
		{
			logger.info("exception while trying to get value from bean and field, bean=" + bean + "\n field=" + field
			        + "\n" + e);
			throw new InvalidAnnotationException("bean=" + bean + "\nfield=" + field, e);
		}
		catch (IllegalAccessException e)
		{
			logger.info("exception while trying to get value from bean and field, bean=" + bean + "\n field=" + field
			        + "\n" + e);
			throw new InvalidAnnotationException("bean=" + bean + "\nfield=" + field, e);
		}
		return value;
	}

	private <T extends Entity> void setFieldValueInBean(T bean, String fieldName, String value)
	        throws InvalidAnnotationException
	{
		Field field = null;
		field = getFieldFromBean(bean, fieldName);
		try
		{
			if (!field.isAccessible())
			{
				field.setAccessible(true);
			}
			field.set(bean, value);
		}
		catch (IllegalArgumentException e)
		{
			logger.info("exception while trying to set value of bean, bean=" + bean + "field=" + field + "value="
			        + value + "\n" + e);
			throw new InvalidAnnotationException("bean=" + bean + "\nfield=" + field, e);
		}
		catch (IllegalAccessException e)
		{
			logger.info("exception while trying to set value of bean, bean=" + bean + "field=" + field + "value="
			        + value + "\n" + e);
			throw new InvalidAnnotationException("bean=" + bean + "\nfield=" + field, e);
		}
	}

	private <T extends Entity> int insertIntoDataStore(DataStoreType type, T bean) throws InvalidAnnotationException,
	        ShardNotAvailableException, JsonSerializationException
	{
		MetaDataCache metaDataCache = getMetaDataCache(type);
		String shardKeyFieldName = metaDataCache.getShardKey(bean.getClass());
		String shardKey = getFieldValueFromBean(bean, shardKeyFieldName);;
		String shardId = shardIdentifier.getShardId(shardKey);
		PersistenceManager pm = getPersistenceManager(type, shardId);
		int rowsUpdated = pm.insert(bean);
		return rowsUpdated;
	}

	private <T extends Entity> String getShardKey(DataStoreType type, T bean) throws InvalidAnnotationException
	{
		MetaDataCache metaDataCache = getMetaDataCache(type);
		String shardKeyFieldName = metaDataCache.getShardKey(bean.getClass());
		String shardKey;
		shardKey = getFieldValueFromBean(bean, shardKeyFieldName);
		return shardKey;
	}

	private ShardLifeCycleManagerInterface getShardLifeCycleManager()
	{
		return this.shardLifeCycleManager;
	}

	private <T extends Entity> T generateShardIdAndUpdateBean(DataStoreType type, T bean)
	        throws InvalidAnnotationException
	{
		MetaDataCache metaDataCache = getMetaDataCache(type);
		String shardKeyFieldName = metaDataCache.getShardKey(bean.getClass());
		String shardKey = getFieldValueFromBean(bean, shardKeyFieldName);
		ShardLifeCycleManagerInterface shardLifeCycleManager = getShardLifeCycleManager();
		List<String> liveShards = shardLifeCycleManager.getShardListForStatus(type, ShardStatus.AVAILABLE_FOR_WRITE);
		String shardId = shardIdentifier.generateShardId(shardKey, liveShards);
		String newShardKey = shardIdentifier.generateNewShardKey(shardKey, shardId);
		setFieldValueInBean(bean, shardKeyFieldName, newShardKey);
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
		logger.debug("Entry,  bean=" + bean);
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
		logger.debug("Entry,  bean=" + bean + "generateShardId=" + generateShardId);
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
				logger.info("Caught exception while trying to insert bean=" + bean + "\n into data store" + type + "\n"
				        + e);
				FailureAction failureAction = writeConfig.getFailureAction();
				if (failureAction == FailureAction.ABORT)
				{
					throw e;
				}
				continue;
			}
			catch (ShardNotAvailableException e)
			{
				logger.info("Caught exception while trying to insert bean=" + bean + "\n into data store" + type + "\n"
				        + e);
				FailureAction failureAction = writeConfig.getFailureAction();
				if (failureAction == FailureAction.ABORT)
				{
					throw e;
				}
				continue;
			}
			catch (JsonSerializationException e)
			{
				logger.info("Caught exception while trying to insert bean=" + bean + "\n into data store" + type + "\n"
				        + e);
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
		logger.debug("Entry,  bean=" + bean);
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
		ReadConfig readConfig = getReadConfigForEntity(clazz);
		List<DataStoreType> readOrder = readConfig.getReadOrder();
		for (DataStoreType type : readOrder)
		{
			MetaDataCache metaDataCache = getMetaDataCache(type);
			String shardKeyFieldName = metaDataCache.getShardKey(clazz);
			if (criteria.containsKey(shardKeyFieldName))
			{
				String shardKey = (String) criteria.get(shardKeyFieldName);
				String shardId = shardIdentifier.getShardId(shardKey);
				PersistenceManager pm = getPersistenceManager(type, shardId);
				try
				{
					result = pm.getByCriteria(clazz, criteria);
				}
				catch (QueryExecutionException e)
				{
					logger.info("Encountered exception while trying to execute query \n DataStoreType=" + type
					        + "\ncriteria=" + criteria + "\nexception=" + e);
					continue;
				}
				return result;
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
				return result;
			}
		}
		return null;
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
