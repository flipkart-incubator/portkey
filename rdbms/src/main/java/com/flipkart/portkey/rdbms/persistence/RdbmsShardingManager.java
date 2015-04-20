package com.flipkart.portkey.rdbms.persistence;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.common.persistence.ShardingManager;
import com.flipkart.portkey.common.persistence.query.SqlUpdateQuery;
import com.flipkart.portkey.common.persistence.query.UpdateQuery;
import com.flipkart.portkey.common.sharding.ShardIdentifier;
import com.flipkart.portkey.common.sharding.ShardLifeCycleManager;
import com.flipkart.portkey.common.sharding.ShardLifeCycleManagerImpl;
import com.flipkart.portkey.common.util.PortKeyUtils;
import com.flipkart.portkey.rdbms.mapper.RdbmsMapper;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;
import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;
import com.flipkart.portkey.rdbms.querybuilder.RdbmsQueryBuilder;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class RdbmsShardingManager implements ShardingManager
{
	private Map<String, RdbmsDatabaseConfig> databaseNameToDatabaseConfigMap;
	private RdbmsMetaDataCache metaDataCache = RdbmsMetaDataCache.getInstance();
	private ShardLifeCycleManager shardLifeCycleManager = ShardLifeCycleManagerImpl.getInstance(DataStoreType.RDBMS);

	public void setDatabaseNameToDatabaseConfigMap(Map<String, RdbmsDatabaseConfig> databaseNameToDatabaseConfigMap)
	{
		this.databaseNameToDatabaseConfigMap = databaseNameToDatabaseConfigMap;
	}

	public void addDatabaseConfig(String databaseName, RdbmsSingleShardedDatabaseConfig dbConfig)
	{
		databaseNameToDatabaseConfigMap.put(databaseName, dbConfig);
	}

	@Override
	public <T extends Entity> T generateShardIdAndUpdateBean(T bean) throws ShardNotAvailableException
	{
		String databaseName = metaDataCache.getMetaData(bean.getClass()).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		return databaseConfig.generateShardIdAndUpdateBean(bean);
	}

	private Map<String, Object> generateColumnToValueMap(Entity obj, RdbmsTableMetaData metaData)
	{
		List<Field> fieldsList = metaData.getFieldsList();
		Map<String, Object> columnToValueMap = new HashMap<String, Object>();
		for (Field field : fieldsList)
		{
			RdbmsField rdbmsField = field.getAnnotation(RdbmsField.class);
			if (rdbmsField != null)
			{
				Object value;
				value = RdbmsMapper.get(obj, field.getName());
				columnToValueMap.put(rdbmsField.columnName(), value);
			}
		}
		return columnToValueMap;
	}

	private List<String> generateColumnsListFromFieldNamesList(RdbmsTableMetaData metaData, List<String> fieldNamesList)
	{
		List<String> columnsList = new ArrayList<String>();
		for (String fieldName : fieldNamesList)
		{
			columnsList.add(metaData.getColumnNameFromFieldName(fieldName));
		}
		return columnsList;
	}

	private <T extends Entity> Map<String, Object> generateColumnToValueMap(Class<T> clazz,
	        Map<String, Object> fieldNameToValueMap)
	{
		Map<String, Object> columnToValueMap = new HashMap<String, Object>();
		RdbmsTableMetaData metaData = metaDataCache.getMetaData(clazz);
		for (String fieldName : fieldNameToValueMap.keySet())
		{
			String columnName = metaData.getColumnNameFromFieldName(fieldName);
			Object valueBeforeSerialization = fieldNameToValueMap.get(fieldName);
			Object value = RdbmsMapper.get(clazz, fieldName, valueBeforeSerialization);
			columnToValueMap.put(columnName, value);
		}
		return columnToValueMap;
	}

	@Override
	public <T extends Entity> int insert(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getMetaData(bean.getClass());
		String databaseName = metaData.getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String insertQuery = RdbmsQueryBuilder.getInstance().getInsertQuery(metaData);
		Map<String, Object> columnToValueMap = generateColumnToValueMap(bean, metaData);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(bean);
		return pm.executeUpdate(insertQuery, columnToValueMap);
	}

	@Override
	public <T extends Entity> int upsert(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getMetaData(bean.getClass());
		String databaseName = metaData.getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String upsertQuery = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData);
		Map<String, Object> columnToValueMap = generateColumnToValueMap(bean, metaData);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(bean);
		return pm.executeUpdate(upsertQuery, columnToValueMap);
	}

	@Override
	public <T extends Entity> int upsert(T bean, List<String> columnsToBeUpdatedOnDuplicate)
	        throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getMetaData(bean.getClass());
		String databaseName = metaData.getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String upsertQuery = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData, columnsToBeUpdatedOnDuplicate);
		Map<String, Object> columnToValueMap = generateColumnToValueMap(bean, metaData);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(bean);
		return pm.executeUpdate(upsertQuery, columnToValueMap);
	}

	@Override
	public <T extends Entity> int update(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getMetaData(bean.getClass());
		String databaseName = metaData.getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String updateQuery = RdbmsQueryBuilder.getInstance().getUpdateByPkQuery(metaData);
		Map<String, Object> columnToValueMap = generateColumnToValueMap(bean, metaData);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(bean);
		return pm.executeUpdate(updateQuery, columnToValueMap);
	}

	@Override
	public <T extends Entity> int update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		int rowsUpdated = 0;
		String databaseName = metaDataCache.getMetaData(clazz).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String shardKeyFieldName = metaDataCache.getShardKeyFieldName(clazz);
		RdbmsTableMetaData metaData = metaDataCache.getMetaData(clazz);
		Map<String, Object> updateColumnToValueMap = generateColumnToValueMap(clazz, updateValuesMap);
		Map<String, Object> criteriaColumnToValueMap = generateColumnToValueMap(clazz, criteria);
		String tableName = metaData.getTableName();
		Map<String, Object> columnToValueMap = PortKeyUtils.mergeMaps(updateColumnToValueMap, criteriaColumnToValueMap);
		List<String> columnsToBeUpdated = new ArrayList<String>(updateColumnToValueMap.keySet());
		List<String> columnsInCriteria = new ArrayList<String>(criteriaColumnToValueMap.keySet());
		String updateQuery =
		        RdbmsQueryBuilder.getInstance().getUpdateByCriteriaQuery(tableName, columnsToBeUpdated,
		                columnsInCriteria, columnToValueMap);
		if (criteria.containsKey(shardKeyFieldName))
		{
			String shardKey = PortKeyUtils.toString(criteria.get(shardKeyFieldName));
			RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardKey);
			return pm.executeUpdate(updateQuery, columnToValueMap);
		}
		else
		{
			List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
			for (RdbmsPersistenceManager pm : persistenceManagersList)
			{
				return pm.executeUpdate(updateQuery, columnToValueMap);
			}
		}
		return rowsUpdated;
	}

	private <T extends Entity> String getShardId(String shardKey, RdbmsTableMetaData metaData)
	        throws ShardNotAvailableException
	{
		String databaseName = metaData.getDatabaseName();
		List<String> liveShards =
		        shardLifeCycleManager.getShardListForStatus(DataStoreType.RDBMS, databaseName,
		                ShardStatus.AVAILABLE_FOR_WRITE);
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		ShardIdentifier shardIdentifier = databaseConfig.getShardIdentifier();
		String shardId = shardIdentifier.getShardId(shardKey, liveShards);
		return shardId;
	}

	private SqlUpdateQuery getSqlUpdateQueryFromUpdateQuery(UpdateQuery query)
	{
		SqlUpdateQuery sqlQuery = new SqlUpdateQuery();
		RdbmsTableMetaData metaData = metaDataCache.getMetaData(query.getClazz());
		String tableName = metaData.getTableName();
		Map<String, Object> criteriaFieldNameToValueMap = query.getCriteriaFieldNameToValueMap();
		Map<String, Object> criteriaColumnToValueMap =
		        generateColumnToValueMap(query.getClazz(), criteriaFieldNameToValueMap);
		Map<String, Object> updateFieldNameToValueMap = query.getUpdateFieldNameToValueMap();
		Map<String, Object> updateColumnToValueMap =
		        generateColumnToValueMap(query.getClazz(), updateFieldNameToValueMap);
		Map<String, Object> columnToValueMap = PortKeyUtils.mergeMaps(criteriaColumnToValueMap, updateColumnToValueMap);
		ArrayList<String> columnsToBeUpdated = new ArrayList<String>(updateColumnToValueMap.keySet());
		ArrayList<String> columnsInCriteria = new ArrayList<String>(criteriaColumnToValueMap.keySet());
		String queryString =
		        RdbmsQueryBuilder.getInstance().getUpdateByCriteriaQuery(tableName, columnsToBeUpdated,
		                columnsInCriteria, columnToValueMap);
		sqlQuery.setQuery(queryString);
		sqlQuery.setColumnToValueMap(columnToValueMap);
		return sqlQuery;
	}

	@Override
	// TODO:SANTOSH:correct the return type of this method
	public <T extends Entity> void update(List<UpdateQuery> updates) throws QueryExecutionException
	{
		Table<String, String, List<SqlUpdateQuery>> databaseToShardIdToUpdateListTable = HashBasedTable.create();
		for (UpdateQuery update : updates)
		{
			String shardKeyFieldName = metaDataCache.getShardKeyFieldName(update.getClazz());
			RdbmsTableMetaData metaData = metaDataCache.getMetaData(update.getClazz());
			Map<String, Object> criteria = update.getCriteriaFieldNameToValueMap();
			if (criteria.containsKey(shardKeyFieldName))
			{
				String databaseName = metaData.getDatabaseName();
				String shardId = getShardId(PortKeyUtils.toString(criteria.get(shardKeyFieldName)), metaData);
				SqlUpdateQuery sqlUpdateQuery = getSqlUpdateQueryFromUpdateQuery(update);
				if (!databaseToShardIdToUpdateListTable.contains(databaseName, shardId))
				{
					databaseToShardIdToUpdateListTable.put(databaseName, shardId, new ArrayList<SqlUpdateQuery>());
				}
				databaseToShardIdToUpdateListTable.get(databaseName, shardId).add(sqlUpdateQuery);
			}
			else
			{
				throw new QueryExecutionException(
				        "Atomic update queries with no shard identifier field in criteria are not supported");
			}
		}
		for (String databaseName : databaseToShardIdToUpdateListTable.rowKeySet())
		{
			RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
			Map<String, List<SqlUpdateQuery>> shardIdToUpdateListMap =
			        databaseToShardIdToUpdateListTable.column(databaseName);
			for (String shardId : shardIdToUpdateListMap.keySet())
			{
				RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardId);
				pm.executeAtomicUpdates(shardIdToUpdateListMap.get(shardId));
			}
		}
	}

	@Override
	public <T extends Entity> int delete(Class<T> clazz, Map<String, Object> criteria) throws QueryExecutionException
	{
		int rowsDeleted = 0;
		String databaseName = metaDataCache.getMetaData(clazz).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		RdbmsTableMetaData metaData = metaDataCache.getMetaData(clazz);
		String tableName = metaData.getTableName();
		String shardKeyFieldName = metaDataCache.getShardKeyFieldName(clazz);
		Map<String, Object> deleteCriteriaColumnToValueMap = generateColumnToValueMap(clazz, criteria);
		String deleteQuery =
		        RdbmsQueryBuilder.getInstance().getDeleteByCriteriaQuery(tableName, deleteCriteriaColumnToValueMap);
		if (criteria.containsKey(shardKeyFieldName))
		{
			String shardKey = PortKeyUtils.toString(criteria.get(shardKeyFieldName));
			RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardKey);
			rowsDeleted = pm.executeUpdate(deleteQuery, deleteCriteriaColumnToValueMap);
		}
		else
		{
			List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
			for (RdbmsPersistenceManager pm : persistenceManagersList)
			{
				rowsDeleted += pm.executeUpdate(deleteQuery, deleteCriteriaColumnToValueMap);
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
		RdbmsTableMetaData metaData = metaDataCache.getMetaData(clazz);
		String tableName = metaData.getTableName();
		Map<String, Object> criteriaColumnToValueMap = generateColumnToValueMap(clazz, criteria);
		String getQuery = RdbmsQueryBuilder.getInstance().getGetByCriteriaQuery(tableName, criteriaColumnToValueMap);
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);

		if (criteria.containsKey(shardKeyFieldName))
		{
			String shardKey = PortKeyUtils.toString(criteria.get(shardKeyFieldName));
			RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardKey);
			return pm.executeQuery(readMaster, getQuery, criteriaColumnToValueMap, mapper);
		}
		else
		{
			List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
			for (RdbmsPersistenceManager pm : persistenceManagersList)
			{
				result.addAll(pm.executeQuery(readMaster, getQuery, criteriaColumnToValueMap, mapper));
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
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> fieldsInSelect,
	        Map<String, Object> criteria, boolean readMaster) throws QueryExecutionException
	{
		List<T> result = new ArrayList<T>();
		String databaseName = metaDataCache.getMetaData(clazz).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String shardKeyFieldName = metaDataCache.getShardKeyFieldName(clazz);
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		String tableName = metaData.getTableName();
		List<String> columnsInSelect = generateColumnsListFromFieldNamesList(metaData, fieldsInSelect);
		Map<String, Object> criteriaColumnToValueMap = generateColumnToValueMap(clazz, criteria);
		String getQuery =
		        RdbmsQueryBuilder.getInstance().getGetByCriteriaQuery(tableName, columnsInSelect,
		                criteriaColumnToValueMap);
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		if (criteria.containsKey(shardKeyFieldName))
		{
			String shardKey = PortKeyUtils.toString(criteria.get(shardKeyFieldName));
			RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardKey);
			return pm.executeQuery(readMaster, getQuery, criteriaColumnToValueMap, mapper);
		}
		else
		{
			List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
			for (RdbmsPersistenceManager pm : persistenceManagersList)
			{
				result.addAll(pm.executeQuery(readMaster, getQuery, criteriaColumnToValueMap, mapper));
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
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		if (criteria != null && criteria.containsKey(shardKeyFieldName))
		{
			String shardKey = PortKeyUtils.toString(criteria.get(shardKeyFieldName));
			RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardKey);
			result = pm.executeQuery(readMaster, sql, criteria, mapper);
		}
		else
		{
			List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
			for (RdbmsPersistenceManager pm : persistenceManagersList)
			{
				result.addAll(pm.executeQuery(readMaster, sql, criteria, mapper));
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
			result.addAll(pm.executeQuery(sql, criteria, readMaster));
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
			rowsUpdated += pm.executeUpdate(sql, criteria);
		}
		return rowsUpdated;
	}

	@Override
	public void healthCheck()

	{
		for (String databaseName : databaseNameToDatabaseConfigMap.keySet())
		{
			RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
			Map<String, ShardStatus> shardStatusMap = databaseConfig.healthCheck();
			shardLifeCycleManager.setShardStatusMap(DataStoreType.RDBMS, databaseName, shardStatusMap);
		}
	}
}
