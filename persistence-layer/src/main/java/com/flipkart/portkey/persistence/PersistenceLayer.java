package com.flipkart.portkey.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.entity.JoinEntity;
import com.flipkart.portkey.common.entity.persistence.EntityPersistencePreference;
import com.flipkart.portkey.common.entity.persistence.ReadConfig;
import com.flipkart.portkey.common.entity.persistence.WriteConfig;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.FailureAction;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.exception.QueryNotSupportedException;
import com.flipkart.portkey.common.persistence.PersistenceLayerInterface;
import com.flipkart.portkey.common.persistence.Result;
import com.flipkart.portkey.common.persistence.ShardingManager;
import com.flipkart.portkey.common.persistence.TransactionManager;
import com.flipkart.portkey.common.persistence.query.UpdateQuery;

/**
 * @author santosh.p
 */
public class PersistenceLayer implements PersistenceLayerInterface, InitializingBean
{
	private static final Logger logger = LoggerFactory.getLogger(PersistenceLayer.class);
	private EntityPersistencePreference defaultPersistencePreference;
	private Map<Class<? extends Entity>, EntityPersistencePreference> entityPersistencePreferenceMap =
	        new HashMap<Class<? extends Entity>, EntityPersistencePreference>();
	private Map<DataStoreType, ShardingManager> dataStoreTypeToShardingManagerMap =
	        new HashMap<DataStoreType, ShardingManager>();
	private ScheduledExecutorService scheduledThreadPool;
	private int healthCheckIntervalInSeconds = 15;

	private enum DBOpeartion
	{
		INSERT, UPSERT, UPDATE;
	};

	private enum UpdateOperation
	{
		UPDATE, DELETE;
	};

	class HealthChecker implements Runnable
	{
		@Override
		public void run()
		{
			for (DataStoreType type : dataStoreTypeToShardingManagerMap.keySet())
			{
				ShardingManager shardingManager = dataStoreTypeToShardingManagerMap.get(type);
				shardingManager.healthCheck();
			}
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

	public void setDataStoreTypeToShardingManagerMap(
	        Map<DataStoreType, ShardingManager> dataStoreTypeToShardingManagerMap)
	{
		this.dataStoreTypeToShardingManagerMap = dataStoreTypeToShardingManagerMap;
	}

	@Override
	public void afterPropertiesSet()
	{
		Assert.notNull(defaultPersistencePreference);
		Assert.notNull(dataStoreTypeToShardingManagerMap);

		logger.info("Initializing Shard life cycle manager");
		scheduledThreadPool = Executors.newScheduledThreadPool(5);
		HealthChecker healthChecker = new HealthChecker();
		healthChecker.run();
		scheduledThreadPool.scheduleAtFixedRate(healthChecker, 0, healthCheckIntervalInSeconds, TimeUnit.SECONDS);
		logger.info("Scheduled health checker");
		logger.info("Initialized Persistence Layer");
	}

	// TODO:SANTOSH: Implement this properly
	public void shutdown()
	{
		scheduledThreadPool.shutdown();
	}

	private EntityPersistencePreference getDefaultPersistencePreference()
	{
		return defaultPersistencePreference;
	}

	private ReadConfig getDefaultReadConfig()
	{
		return getDefaultPersistencePreference().getReadConfig();
	}

	private WriteConfig getDefaultWriteConfig()
	{
		return getDefaultPersistencePreference().getWriteConfig();
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

	private ShardingManager getShardingManager(DataStoreType type)
	{
		return dataStoreTypeToShardingManagerMap.get(type);
	}

	private <T extends Entity> int insertIntoDataStore(DataStoreType type, T bean) throws QueryExecutionException
	{
		return getShardingManager(type).insert(bean);
	}

	private <T extends Entity> int updateIntoDataStore(DataStoreType type, T bean) throws QueryExecutionException
	{
		return getShardingManager(type).update(bean);
	}

	private <T extends Entity> int upsertIntoDataStore(DataStoreType type, T bean) throws QueryExecutionException
	{
		return getShardingManager(type).upsert(bean);
	}

	private <T extends Entity> int upsertIntoDataStore(DataStoreType type, T bean,
	        List<String> columnsToBeUpdatedOnDuplicate) throws QueryExecutionException
	{
		return getShardingManager(type).upsert(bean, columnsToBeUpdatedOnDuplicate);
	}

	private <T extends Entity> Result performDBOperation(DBOpeartion operation, T bean) throws QueryExecutionException
	{
		return performDBOperation(operation, bean, null);
	}

	private <T extends Entity> Result performDBOperation(DBOpeartion operation, T bean,
	        List<String> columnsToBeUpdatedOnDuplicate) throws QueryExecutionException
	{
		Result result = new Result();
		WriteConfig writeConfig = getWriteConfigForEntity(bean.getClass());
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();

		for (DataStoreType dataStoreType : writeOrder)
		{
			int rowsUpdated = 0;
			try
			{
				if (operation.equals(DBOpeartion.INSERT))
				{
					rowsUpdated = insertIntoDataStore(dataStoreType, bean);
				}
				else if (operation.equals(DBOpeartion.UPDATE))
				{
					rowsUpdated = updateIntoDataStore(dataStoreType, bean);
				}
				else if (operation.equals(DBOpeartion.UPSERT))
				{
					if (columnsToBeUpdatedOnDuplicate == null)
					{
						rowsUpdated = upsertIntoDataStore(dataStoreType, bean);
					}
					else
					{
						rowsUpdated = upsertIntoDataStore(dataStoreType, bean, columnsToBeUpdatedOnDuplicate);
					}
				}
			}
			catch (QueryExecutionException e)
			{
				logger.warn("Exception while trying to " + operation + " bean into data store, bean=" + bean
				        + ", data store type=" + dataStoreType, e);
				FailureAction failureAction = writeConfig.getFailureAction();
				if (failureAction != FailureAction.CONTINUE)
				{
					throw new QueryExecutionException("Failed to " + operation + " bean into datastore, bean=" + bean
					        + "data store type=" + dataStoreType, e);
				}
			}
			result.setRowsUpdatedForDataStore(dataStoreType, rowsUpdated);
		}
		result.setEntity(bean);
		return result;
	}

	@Override
	public <T extends Entity> Result insert(T bean) throws QueryExecutionException
	{
		return insert(bean, false);
	}

	@Override
	public <T extends Entity> Result insert(T bean, boolean generateShardId) throws QueryExecutionException
	{
		if (generateShardId)
		{
			WriteConfig writeConfig = getWriteConfigForEntity(bean.getClass());
			List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
			DataStoreType dataStoreTypeForShardIdGeneration = writeOrder.get(0);
			ShardingManager shardingManager = getShardingManager(dataStoreTypeForShardIdGeneration);
			bean = shardingManager.generateShardIdAndUpdateBean(bean);
		}
		return performDBOperation(DBOpeartion.INSERT, bean);
	}

	@Override
	public <T extends Entity> void insert(List<T> beans) throws PortKeyException
	{
		WriteConfig writeConfig = getDefaultWriteConfig();
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType type : writeOrder)
		{
			ShardingManager shardingManager = dataStoreTypeToShardingManagerMap.get(type);
			try
			{
				shardingManager.insert(beans);
			}
			catch (QueryNotSupportedException e)
			{
				logger.info("Method not supported by " + type + " implementation");
				continue;
			}
		}
	}

	@Override
	public <T extends Entity> Result upsert(T bean) throws QueryExecutionException
	{
		return performDBOperation(DBOpeartion.UPSERT, bean, null);
	}

	@Override
	public <T extends Entity> Result upsert(T bean, List<String> columnsToBeUpdatedOnDuplicate)
	        throws QueryExecutionException
	{
		return performDBOperation(DBOpeartion.UPSERT, bean, columnsToBeUpdatedOnDuplicate);
	}

	@Override
	public <T extends Entity> Result update(T bean) throws QueryExecutionException
	{
		return performDBOperation(DBOpeartion.UPDATE, bean, null);
	}

	private <T extends Entity> int updateIntoDataStore(DataStoreType type, Class<T> clazz,
	        Map<String, Object> updateValuesMap, Map<String, Object> criteria) throws QueryExecutionException
	{
		return getShardingManager(type).update(clazz, updateValuesMap, criteria);
	}

	private <T extends Entity> int deleteFromDataStore(DataStoreType type, Class<T> clazz, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		return getShardingManager(type).delete(clazz, criteria);
	}

	private <T extends Entity> Result performUpdateOperation(UpdateOperation operation, Class<T> clazz,
	        Map<String, Object> updateValuesMap, Map<String, Object> criteria) throws QueryExecutionException
	{
		Result result = new Result();
		WriteConfig writeConfig = getWriteConfigForEntity(clazz);
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType dataStoreType : writeOrder)
		{
			int rowsUpdated = 0;
			try
			{
				if (operation.equals(UpdateOperation.UPDATE))
				{
					rowsUpdated = updateIntoDataStore(dataStoreType, clazz, updateValuesMap, criteria);
				}
				else if (operation.equals(UpdateOperation.DELETE))
				{
					rowsUpdated = deleteFromDataStore(dataStoreType, clazz, criteria);
				}
			}
			catch (QueryExecutionException e)
			{
				FailureAction failureAction = writeConfig.getFailureAction();
				if (failureAction == FailureAction.ABORT)
				{
					throw new QueryExecutionException("Failed to execute update query, class=" + clazz
					        + ", updateValuesMap=" + updateValuesMap + ", criteria=" + criteria + "data store type="
					        + dataStoreType, e);
				}
				logger.warn("Failed to execute update query, class=" + clazz + ", updateValuesMap=" + updateValuesMap
				        + ", criteria=" + criteria + "data store type=" + dataStoreType, e);
			}
			result.setRowsUpdatedForDataStore(dataStoreType, rowsUpdated);
		}
		return result;
	}

	@Override
	public <T extends Entity> Result update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		return performUpdateOperation(UpdateOperation.UPDATE, clazz, updateValuesMap, criteria);
	}

	@Override
	public <T extends Entity> Result delete(Class<T> clazz, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		return performUpdateOperation(UpdateOperation.DELETE, clazz, null, criteria);
	}

	@Override
	public Result update(List<UpdateQuery> queries) throws PortKeyException
	{
		Result result = new Result();
		WriteConfig writeConfig = getDefaultWriteConfig();
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType type : writeOrder)
		{
			ShardingManager shardingManager = dataStoreTypeToShardingManagerMap.get(type);
			try
			{
				int rowsUpdated = shardingManager.update(queries);
				result.setRowsUpdatedForDataStore(type, rowsUpdated);
			}
			catch (QueryNotSupportedException e)
			{
				logger.info("Method not supported by " + type + " implementation");
				continue;
			}
		}
		return result;
	}

	@Override
	public Result update(List<UpdateQuery> queries, boolean failIfNoRowsAreUpdated) throws PortKeyException
	{
		Result result = new Result();
		WriteConfig writeConfig = getDefaultWriteConfig();
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType type : writeOrder)
		{
			ShardingManager shardingManager = dataStoreTypeToShardingManagerMap.get(type);
			try
			{
				int rowsUpdated = shardingManager.update(queries, failIfNoRowsAreUpdated);
				result.setRowsUpdatedForDataStore(type, rowsUpdated);
			}
			catch (QueryNotSupportedException e)
			{
				logger.info("Method not supported by " + type + " implementation");
				continue;
			}
		}
		return result;
	}

	private <T extends Entity> List<T> performGetByCriteriaQuery(Class<T> clazz, List<String> fieldNameList,
	        Map<String, Object> criteriaMap) throws QueryExecutionException
	{
		List<T> result = new ArrayList<T>();
		ReadConfig readConfig = getReadConfigForEntity(clazz);
		List<DataStoreType> readOrder = readConfig.getReadOrder();
		for (DataStoreType type : readOrder)
		{
			try
			{
				ShardingManager shardingManager = dataStoreTypeToShardingManagerMap.get(type);
				if (fieldNameList == null)
				{
					result = shardingManager.getByCriteria(clazz, criteriaMap);
				}
				else
				{
					result = shardingManager.getByCriteria(clazz, fieldNameList, criteriaMap);
				}
			}
			catch (QueryExecutionException e)
			{
				logger.warn("Exception while trying to fetch from data store:" + type, e);
				continue;
			}
			return result;
		}
		throw new QueryExecutionException("Failed to execute query, class=" + clazz + ", fieldNames=" + fieldNameList
		        + ", criteria" + criteriaMap);
	}

	private <T extends JoinEntity> List<T> performGetByJoinCriteriaQuery(Class<T> clazz, List<String> fieldNameList,
	        Map<String, Object> criteriaMap) throws QueryExecutionException
	{
		List<T> result = new ArrayList<T>();
		ReadConfig readConfig = getReadConfigForEntity(clazz);
		List<DataStoreType> readOrder = readConfig.getReadOrder();
		for (DataStoreType type : readOrder)
		{
			try
			{
				ShardingManager shardingManager = dataStoreTypeToShardingManagerMap.get(type);
				result = shardingManager.getByJoinCriteria(clazz, fieldNameList, criteriaMap);
			}
			catch (QueryExecutionException e)
			{
				logger.warn("Exception while trying to fetch from data store:" + type, e);
				continue;
			}
			return result;
		}
		throw new QueryExecutionException("Failed to execute query, class=" + clazz + ", fieldNames=" + fieldNameList
		        + ", criteria" + criteriaMap);
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteriaMap)
	        throws QueryExecutionException
	{
		return performGetByCriteriaQuery(clazz, null, criteriaMap);
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> fieldNameList,
	        Map<String, Object> criteriaMap) throws QueryExecutionException
	{
		return performGetByCriteriaQuery(clazz, fieldNameList, criteriaMap);
	}

	@Override
	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		List<T> result = new ArrayList<T>();
		ReadConfig readConfig = getReadConfigForEntity(clazz);
		List<DataStoreType> readOrder = readConfig.getReadOrder();
		for (DataStoreType type : readOrder)
		{
			try
			{
				ShardingManager shardingManager = dataStoreTypeToShardingManagerMap.get(type);
				result = shardingManager.getBySql(clazz, sql, criteria);
			}
			catch (QueryExecutionException e)
			{
				logger.warn("Exception while trying to execute sql query " + sql + " on datastore " + type, e);
				continue;
			}
			return result;
		}
		throw new QueryExecutionException("Failed to execute query, class=" + clazz + ", sql=" + sql + ", criteria="
		        + criteria);
	}

	@Override
	public List<Map<String, Object>> getBySql(String databaseName, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		List<Map<String, Object>> result;
		ReadConfig readConfig = getDefaultReadConfig();
		List<DataStoreType> readOrder = readConfig.getReadOrder();
		for (DataStoreType type : readOrder)
		{
			try
			{
				ShardingManager shardingManager = dataStoreTypeToShardingManagerMap.get(type);
				result = shardingManager.getBySql(databaseName, sql, criteria);
			}
			catch (QueryExecutionException e)
			{
				logger.warn("Exception while trying to execute sql query " + sql + " on datastore " + type, e);
				continue;
			}
			return result;
		}
		throw new QueryExecutionException("Failed to execute query, sql=" + sql + ", criteria=" + criteria);
	}

	@Override
	public <T extends Entity> List<T> getBySql(Class<T> clazz, Map<DataStoreType, String> sqlMap,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		List<T> result = null;
		ReadConfig readConfig = getReadConfigForEntity(clazz);
		List<DataStoreType> readOrder = readConfig.getReadOrder();
		for (DataStoreType type : readOrder)
		{
			try
			{
				ShardingManager shardingManager = dataStoreTypeToShardingManagerMap.get(type);
				result = shardingManager.getBySql(clazz, sqlMap.get(type), criteria);
			}
			catch (QueryExecutionException e)
			{
				logger.warn("Failed to execute query " + sqlMap.get(type) + " for datastore" + type, e);
				continue;
			}

			return result;
		}
		throw new QueryExecutionException("Failed to execute query, class=" + clazz + "sqlMap=" + sqlMap
		        + ", criteria=" + criteria);
	}

	@Override
	public List<Map<String, Object>> getBySql(Map<DataStoreType, String> datStoreTypeToDatabaseNameMap,
	        Map<DataStoreType, String> dataStoreTypeToSqlMap, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		List<Map<String, Object>> result = null;
		ReadConfig readConfig = getDefaultReadConfig();
		List<DataStoreType> readOrder = readConfig.getReadOrder();
		for (DataStoreType type : readOrder)
		{

			try
			{
				ShardingManager shardingManager = dataStoreTypeToShardingManagerMap.get(type);
				result =
				        shardingManager.getBySql(datStoreTypeToDatabaseNameMap.get(type),
				                dataStoreTypeToSqlMap.get(type), criteria);
			}
			catch (QueryExecutionException e)
			{
				logger.warn("Failed to execute query " + dataStoreTypeToSqlMap.get(type) + " for datastore" + type, e);
				continue;
			}
			return result;
		}
		throw new QueryExecutionException("Failed to execute query sqlMap=" + dataStoreTypeToSqlMap + ", criteria="
		        + criteria);
	}

	@Override
	public Result updateBySql(String databaseName, String sql, Map<String, Object> criteria) throws PortKeyException
	{
		Result result = new Result();
		int rowsUpdated;
		WriteConfig writeConfig = getDefaultWriteConfig();
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		for (DataStoreType type : writeOrder)
		{
			rowsUpdated = 0;
			try
			{
				ShardingManager shardingManager = dataStoreTypeToShardingManagerMap.get(type);
				rowsUpdated = shardingManager.updateBySql(databaseName, sql, criteria);
				result.setRowsUpdatedForDataStore(type, rowsUpdated);
			}
			catch (QueryExecutionException e)
			{
				logger.warn("Failed to execute update query " + sql + " for datastore " + type, e);
				FailureAction failureAction = writeConfig.getFailureAction();
				if (failureAction.equals(FailureAction.ABORT))
				{
					throw new QueryExecutionException("Failed to execute query, sql=" + sql + ", criteria=" + criteria);
				}
				continue;
			}
			return result;
		}
		throw new QueryExecutionException("Failed to execute query " + sql + ".\nAll related data stores are down");
	}

	public <T extends Entity> TransactionManager getTransactionManager(T bean, DataStoreType type)
	        throws PortKeyException
	{
		return dataStoreTypeToShardingManagerMap.get(type).getTransactionManager(bean);
	}

	public <T extends Entity> TransactionLayer getTransactionLayer(T bean) throws PortKeyException
	{
		WriteConfig writeConfig = getWriteConfigForEntity(bean.getClass());
		List<DataStoreType> writeOrder = writeConfig.getWriteOrder();
		LinkedHashMap<DataStoreType, TransactionManager> dataStoreTypeToTransactionManagerMap =
		        new LinkedHashMap<DataStoreType, TransactionManager>();
		for (DataStoreType type : writeOrder)
		{
			dataStoreTypeToTransactionManagerMap.put(type, dataStoreTypeToShardingManagerMap.get(type)
			        .getTransactionManager(bean));
		}
		return new TransactionLayer(dataStoreTypeToTransactionManagerMap);
	}

	@Override
	public <T extends JoinEntity> List<T> getByJoinCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws PortKeyException
	{
		return performGetByJoinCriteriaQuery(clazz, attributeNames, criteria);
	}
}
