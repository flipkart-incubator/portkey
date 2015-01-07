/**
 * 
 */
package com.flipkart.portkey.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.flipkart.portkey.common.datastore.DataStoreConfig;
import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.entity.persistence.EntityPersistencePreference;
import com.flipkart.portkey.common.entity.persistence.ReadConfig;
import com.flipkart.portkey.common.entity.persistence.WriteConfig;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.FailureAction;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.MethodNotSupportedForDataStoreException;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.exception.QueryNotSupportedException;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.common.metadata.MetaDataCache;
import com.flipkart.portkey.common.persistence.PersistenceLayerInterface;
import com.flipkart.portkey.common.persistence.PersistenceManager;
import com.flipkart.portkey.common.persistence.Result;
import com.flipkart.portkey.common.sharding.ShardIdentifierInterface;
import com.flipkart.portkey.common.sharding.ShardLifeCycleManagerInterface;
import com.flipkart.portkey.common.util.PortKeyUtils;
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
	private Map<DataStoreType, DataStoreConfig> dataStoreConfigMap;
	private ShardIdentifierInterface shardIdentifier;
	private ShardLifeCycleManagerInterface shardLifeCycleManager;
	private ScheduledExecutorService scheduledThreadPool;

	class HealthCheckScheduler implements Runnable
	{
		@Override
		public void run()
		{
			logger.info("running healthckecker");
			// TODO: try fetching a snapshot of shard statuses, instead of individual fetches from shardLifeCycleManager
			for (DataStoreType type : dataStoreConfigMap.keySet())
			{
				DataStoreConfig ds = dataStoreConfigMap.get(type);
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

	public void setDataStoresMap(Map<DataStoreType, DataStoreConfig> dataStoresMap)
	{
		this.dataStoreConfigMap = dataStoresMap;
	}

	@Override
	public void afterPropertiesSet() throws Exception
	{
		Assert.notNull(defaultPersistencePreference);
		Assert.notNull(dataStoreConfigMap);

		logger.info("Assertions passed");

		shardIdentifier = new ShardIdentifier();
		logger.info("shardIdentifier initialized");

		logger.info("initializing ShardLifeCycleManager");
		List<DataStoreType> dataStoreTypes = new ArrayList<DataStoreType>(dataStoreConfigMap.keySet());
		shardLifeCycleManager = new ShardLifeCycleManager(dataStoreTypes);
		logger.info("number of data stores to be registered=" + dataStoreTypes.size());
		logger.info("datastores=" + dataStoreTypes);
		for (DataStoreType type : dataStoreTypes)
		{
			DataStoreConfig ds = dataStoreConfigMap.get(type);
			List<String> shardIds = ds.getShardIds();
			logger.info("registering " + shardIds.size() + " shards for datastoretype=" + dataStoreTypes);
			for (String shardId : shardIds)
			{
				logger.info("registering shard id=" + shardId);
				shardLifeCycleManager.setShardStatus(type, shardId, ShardStatus.getDefaultStatus());
			}
		}
		logger.info("ShardLifeCycleManager initialized");
		scheduledThreadPool = Executors.newScheduledThreadPool(5);
		HealthCheckScheduler healthCheckScheduler = new HealthCheckScheduler();
		healthCheckScheduler.run();
		scheduledThreadPool.scheduleAtFixedRate(healthCheckScheduler, 0, 100, TimeUnit.SECONDS);
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

	private DataStoreConfig getDataStore(DataStoreType type)
	{
		return dataStoreConfigMap.get(type);
	}

	private PersistenceManager getPersistenceManager(DataStoreType type, String shardId)
	{
		DataStoreConfig ds = getDataStore(type);
		return ds.getPersistenceManager(shardId);
	}

	private MetaDataCache getMetaDataCache(DataStoreType type)
	{
		DataStoreConfig dataStore = getDataStore(type);
		return dataStore.getMetaDataCache();
	}

	private <T extends Entity> int insertIntoDataStore(DataStoreType type, T bean) throws QueryExecutionException
	{
		MetaDataCache metaDataCache = getMetaDataCache(type);
		String shardKeyFieldName = metaDataCache.getShardKey(bean.getClass());
		String shardKey = PortKeyUtils.toString(PortKeyUtils.getFieldValueFromBean(bean, shardKeyFieldName));
		String shardId = shardIdentifier.getShardId(shardKey);
		PersistenceManager pm = getPersistenceManager(type, shardId);
		int rowsUpdated = pm.insert(bean);
		return rowsUpdated;
	}

	private <T extends Entity> String getShardKey(DataStoreType type, T bean)
	{
		MetaDataCache metaDataCache = getMetaDataCache(type);
		String shardKeyFieldName = metaDataCache.getShardKey(bean.getClass());
		String shardKey;
		shardKey = PortKeyUtils.toString(PortKeyUtils.getFieldValueFromBean(bean, shardKeyFieldName));
		return shardKey;
	}

	private ShardLifeCycleManagerInterface getShardLifeCycleManager()
	{
		return this.shardLifeCycleManager;
	}

	private <T extends Entity> T generateShardIdAndUpdateBean(DataStoreType type, T bean)
	{
		MetaDataCache metaDataCache = getMetaDataCache(type);
		String shardKeyFieldName = metaDataCache.getShardKey(bean.getClass());
		String shardKey = PortKeyUtils.toString(PortKeyUtils.getFieldValueFromBean(bean, shardKeyFieldName));
		ShardLifeCycleManagerInterface shardLifeCycleManager = getShardLifeCycleManager();
		List<String> liveShards = shardLifeCycleManager.getShardListForStatus(type, ShardStatus.AVAILABLE_FOR_WRITE);
		String shardId = shardIdentifier.generateShardId(shardKey, liveShards);
		String newShardKey = shardIdentifier.generateNewShardKey(shardKey, shardId);
		PortKeyUtils.setFieldValueInBean(bean, shardKeyFieldName, newShardKey);
		return bean;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#insert(com.flipkart.portkey.common.entity.Entity
	 * )
	 */
	public <T extends Entity> Result insert(T bean) throws QueryExecutionException
	{
		return insert(bean, false);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#insert(com.flipkart.portkey.common.entity.Entity
	 * , boolean)
	 */
	public <T extends Entity> Result insert(T bean, boolean generateShardId) throws QueryExecutionException
	{
		Result result = new Result();
		WriteConfig writeConfig = getWriteConfigForEntity(bean.getClass());
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		if (generateShardId)
		{
			DataStoreType dataStoreTypeForShardIdGeneration = writeOrder.get(0);
			bean = generateShardIdAndUpdateBean(dataStoreTypeForShardIdGeneration, bean);
		}

		for (DataStoreType dataStoreType : writeOrder)
		{
			int rowsUpdated = 0;
			try
			{
				rowsUpdated = insertIntoDataStore(dataStoreType, bean);
			}
			catch (QueryExecutionException e)
			{
				logger.info("Caught exception while trying to insert bean=" + bean + "\n into data store"
				        + dataStoreType + "\n" + e);
				FailureAction failureAction = writeConfig.getFailureAction();
				if (failureAction == FailureAction.ABORT)
				{
					throw new QueryExecutionException("Exception while inserting bean into datastore, bean=" + bean
					        + "\ndatastoretype=" + dataStoreType, e);
				}
				continue;
			}
			result.setRowsUpdatedForDataStore(dataStoreType, rowsUpdated);
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
	public <T extends Entity> Result update(T bean) throws QueryExecutionException
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
			int rowsUpdated = 0;
			try
			{
				rowsUpdated = pm.update(bean);
			}
			catch (QueryExecutionException e)
			{
				logger.info("Caught exception while trying to update bean=" + bean + "\n into data store" + type + "\n"
				        + e);
				FailureAction failureAction = writeConfig.getFailureAction();
				if (failureAction == FailureAction.ABORT)
				{
					throw new QueryExecutionException("Exception while updating bean, bean=" + bean
					        + "\ndatastoretype=" + type, e);
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
				int rowsUpdated = 0;
				try
				{
					rowsUpdated = pm.update(clazz, updateValuesMap, criteria);
				}
				catch (QueryExecutionException e)
				{
					logger.info("Caught exception while trying to execute update " + updateValuesMap + "\n data store:"
					        + type + "\n" + e);
					FailureAction failureAction = writeConfig.getFailureAction();
					if (failureAction == FailureAction.ABORT)
					{
						throw new QueryExecutionException("Exception while trying to execute update " + updateValuesMap
						        + "\n data store:" + type, e);
					}
					continue;
				}
				result.setRowsUpdatedForDataStore(type, rowsUpdated);
			}
			else
			{
				List<String> shardIds = dataStoreConfigMap.get(type).getShardIds();
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
	public <T extends Entity> Result delete(Class<T> clazz, Map<String, Object> criteria)
	        throws ShardNotAvailableException, QueryNotSupportedException
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
				int rowsUpdated = 0;
				String shardKey = (String) criteria.get(shardKeyFieldName);
				String shardId = shardIdentifier.getShardId(shardKey);
				try
				{
					PersistenceManager pm = getPersistenceManager(type, shardId);
					rowsUpdated = pm.delete(clazz, criteria);
					result.setRowsUpdatedForDataStore(type, rowsUpdated);
				}
				catch (QueryNotSupportedException e)
				{
					logger.info("Caught exception while trying to delete from data store" + type + "\n" + e);
					FailureAction failureAction = writeConfig.getFailureAction();
					if (failureAction == FailureAction.ABORT)
					{
						throw new QueryNotSupportedException("Exception while executing delete from datastore, type="
						        + type, e);
					}
					result.setRowsUpdatedForDataStore(type, rowsUpdated);
					continue;
				}
				catch (ShardNotAvailableException e)
				{
					logger.info("Caught exception while trying to delete from data store" + type + "\n" + e);
					FailureAction failureAction = writeConfig.getFailureAction();
					if (failureAction == FailureAction.ABORT)
					{
						throw new QueryNotSupportedException("Exception while executing delete from datastore, type="
						        + type, e);
					}
					result.setRowsUpdatedForDataStore(type, rowsUpdated);
					continue;
				}
			}
			else
			{
				List<String> shardIds = dataStoreConfigMap.get(type).getShardIds();
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
				boolean encounteredExecption = false;
				List<String> shardIds = dataStoreConfigMap.get(type).getShardIds();
				for (String shardId : shardIds)
				{
					PersistenceManager pm = getPersistenceManager(type, shardId);
					List<T> intermediateResult;
					try
					{
						intermediateResult = pm.getByCriteria(clazz, criteria);
					}
					catch (QueryExecutionException e)
					{
						logger.info("Encountered exception while trying to execute query \n DataStoreType=" + type
						        + "\ncriteria=" + criteria + "\nexception=" + e);
						encounteredExecption = true;
						break;
					}
					result.addAll(intermediateResult);
				}
				if (encounteredExecption)
				{
					continue;
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
				List<String> shardIds = dataStoreConfigMap.get(type).getShardIds();
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
		ReadConfig readConfig = getReadConfigForEntity(clazz);
		List<DataStoreType> readOrder = readConfig.getReadOrder();
		for (DataStoreType type : readOrder)
		{
			try
			{
				List<T> result = new ArrayList<T>();
				List<String> shardIds = dataStoreConfigMap.get(type).getShardIds();
				for (String shardId : shardIds)
				{
					PersistenceManager pm = getPersistenceManager(type, shardId);
					List<T> intermediateResult = pm.getBySql(clazz, sql, criteria);
					result.addAll(intermediateResult);
				}
				return result;
			}
			catch (MethodNotSupportedForDataStoreException e)
			{
				logger.info("Method not supported by datastore" + type);
				continue;
			}
		}
		return null;
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
			List<String> shardIds = dataStoreConfigMap.get(type).getShardIds();
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
			List<String> shardIds = dataStoreConfigMap.get(type).getShardIds();
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
			List<String> shardIds = dataStoreConfigMap.get(type).getShardIds();
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
