package com.flipkart.portkey.rdbms.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.common.persistence.ShardingManager;
import com.flipkart.portkey.common.util.PortKeyUtils;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;

public class RdbmsShardingManager implements ShardingManager
{
	private Map<String, RdbmsDatabaseConfig> databaseNameToDatabaseConfigMap;
	private RdbmsMetaDataCache metaDataCache;

	public <T extends Entity> T generateShardIdAndUpdateBean(T bean) throws ShardNotAvailableException
	{
		String databaseName = metaDataCache.getMetaData(bean.getClass()).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		return databaseConfig.generateShardIdAndUpdateBean(bean);
	}

	@Override
	public <T extends Entity> int insert(T bean) throws QueryExecutionException
	{
		String databaseName = metaDataCache.getMetaData(bean.getClass()).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(bean);
		return pm.insert(bean);
	}

	@Override
	public <T extends Entity> int upsert(T bean) throws QueryExecutionException
	{
		String databaseName = metaDataCache.getMetaData(bean.getClass()).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(bean);
		return pm.upsert(bean);
	}

	@Override
	public <T extends Entity> int upsert(T bean, List<String> columnsToBeUpdatedOnDuplicate)
	        throws QueryExecutionException
	{
		String databaseName = metaDataCache.getMetaData(bean.getClass()).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(bean);
		return pm.upsert(bean, columnsToBeUpdatedOnDuplicate);
	}

	@Override
	public <T extends Entity> int update(T bean) throws QueryExecutionException
	{
		String databaseName = metaDataCache.getMetaData(bean.getClass()).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(bean);
		return pm.update(bean);
	}

	@Override
	public <T extends Entity> int update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		int rowsUpdated = 0;
		String databaseName = metaDataCache.getMetaData(clazz).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String shardKeyFieldName = metaDataCache.getShardKeyFieldName(clazz);
		if (criteria.containsKey(shardKeyFieldName))
		{
			String shardKey = PortKeyUtils.toString(criteria.get(shardKeyFieldName));
			RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardKey);
			rowsUpdated = pm.update(clazz, updateValuesMap, criteria);
		}
		else
		{
			List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
			for (RdbmsPersistenceManager pm : persistenceManagersList)
			{
				rowsUpdated += pm.update(clazz, updateValuesMap, criteria);
			}
		}
		return rowsUpdated;
	}

	@Override
	public <T extends Entity> int delete(Class<T> clazz, Map<String, Object> criteria) throws QueryExecutionException
	{
		int rowsDeleted = 0;
		String databaseName = metaDataCache.getMetaData(clazz).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String shardKeyFieldName = metaDataCache.getShardKeyFieldName(clazz);
		if (criteria.containsKey(shardKeyFieldName))
		{
			String shardKey = PortKeyUtils.toString(criteria.get(shardKeyFieldName));
			RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardKey);
			rowsDeleted = pm.delete(clazz, criteria);
		}
		else
		{
			List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
			for (RdbmsPersistenceManager pm : persistenceManagersList)
			{
				rowsDeleted += pm.delete(clazz, criteria);
			}
		}
		return rowsDeleted;
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		return getByCriteria(clazz, criteria, false);
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria, boolean readMaster)
	        throws QueryExecutionException
	{
		List<T> result = new ArrayList<T>();
		String databaseName = metaDataCache.getMetaData(clazz).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String shardKeyFieldName = metaDataCache.getShardKeyFieldName(clazz);
		if (criteria.containsKey(shardKeyFieldName))
		{
			String shardKey = PortKeyUtils.toString(criteria.get(shardKeyFieldName));
			RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardKey);
			result = pm.getByCriteria(clazz, criteria, readMaster);
		}
		else
		{
			List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
			for (RdbmsPersistenceManager pm : persistenceManagersList)
			{
				result.addAll(pm.getByCriteria(clazz, criteria, readMaster));
			}
		}
		return result;
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		return getByCriteria(clazz, attributeNames, criteria, false);
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria, boolean readMaster) throws QueryExecutionException
	{
		List<T> result = new ArrayList<T>();
		String databaseName = metaDataCache.getMetaData(clazz).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String shardKeyFieldName = metaDataCache.getShardKeyFieldName(clazz);
		if (criteria.containsKey(shardKeyFieldName))
		{
			String shardKey = PortKeyUtils.toString(criteria.get(shardKeyFieldName));
			RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardKey);
			result = pm.getByCriteria(clazz, attributeNames, criteria, readMaster);
		}
		else
		{
			List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
			for (RdbmsPersistenceManager pm : persistenceManagersList)
			{
				result.addAll(pm.getByCriteria(clazz, attributeNames, criteria, readMaster));
			}
		}
		return result;
	}

	@Override
	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		return getBySql(clazz, sql, criteria, false);
	}

	@Override
	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria,
	        boolean readMaster) throws QueryExecutionException
	{
		List<T> result = new ArrayList<T>();
		String databaseName = metaDataCache.getMetaData(clazz).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String shardKeyFieldName = metaDataCache.getShardKeyFieldName(clazz);
		if (criteria.containsKey(shardKeyFieldName))
		{
			String shardKey = PortKeyUtils.toString(criteria.get(shardKeyFieldName));
			RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardKey);
			result = pm.getBySql(clazz, sql, criteria, readMaster);
		}
		else
		{
			List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
			for (RdbmsPersistenceManager pm : persistenceManagersList)
			{
				result.addAll(pm.getBySql(clazz, sql, criteria, readMaster));
			}
		}
		return result;
	}

	@Override
	public List<Map<String, Object>> getBySql(String databaseName, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		return getBySql(databaseName, sql, criteria, false);
	}

	@Override
	public List<Map<String, Object>> getBySql(String databaseName, String sql, Map<String, Object> criteria,
	        boolean readMaster) throws QueryExecutionException
	{
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
		for (RdbmsPersistenceManager pm : persistenceManagersList)
		{
			result.addAll(pm.getBySql(sql, criteria, readMaster));
		}
		return result;
	}

	@Override
	public int updateBySql(String databaseName, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		int rowsUpdated = 0;
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
		for (RdbmsPersistenceManager pm : persistenceManagersList)
		{
			rowsUpdated += pm.updateBySql(sql, criteria);
		}
		return rowsUpdated;
	}

	@Override
	public void healthCheck()
	{
		for (String databaseName : databaseNameToDatabaseConfigMap.keySet())
		{
			RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
			databaseConfig.healthCheck();
		}
	}

}
