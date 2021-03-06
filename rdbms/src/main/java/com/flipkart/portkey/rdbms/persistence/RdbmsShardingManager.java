package com.flipkart.portkey.rdbms.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.entity.JoinEntity;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.exception.QueryNotSupportedException;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.common.persistence.ShardingManager;
import com.flipkart.portkey.common.persistence.query.SqlQuery;
import com.flipkart.portkey.common.persistence.query.UpdateQuery;
import com.flipkart.portkey.common.sharding.ShardIdentifier;
import com.flipkart.portkey.common.sharding.ShardLifeCycleManager;
import com.flipkart.portkey.common.sharding.ShardLifeCycleManagerImpl;
import com.flipkart.portkey.common.util.PortKeyUtils;
import com.flipkart.portkey.rdbms.mapper.RdbmsJoinMapper;
import com.flipkart.portkey.rdbms.mapper.RdbmsMapper;
import com.flipkart.portkey.rdbms.metadata.RdbmsJoinMetaData;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;
import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;
import com.flipkart.portkey.rdbms.querybuilder.RdbmsQueryBuilder;
import com.flipkart.portkey.rdbms.transaction.RdbmsTransactionManager;

public class RdbmsShardingManager implements ShardingManager
{
	private static Logger logger = LoggerFactory.getLogger(RdbmsShardingManager.class);
	private Map<String, RdbmsDatabaseConfig> databaseNameToDatabaseConfigMap;
	private ShardLifeCycleManager shardLifeCycleManager = ShardLifeCycleManagerImpl.getInstance(DataStoreType.RDBMS);
	private RdbmsMetaDataCache metaDataCache = RdbmsMetaDataCache.getInstance();
	private static StopWatch stopwatch = new Slf4JStopWatch(logger);

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
		String databaseName = RdbmsMetaDataCache.getInstance().getTableMetaData(bean.getClass()).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		return databaseConfig.generateShardIdAndUpdateBean(bean);
	}

	@Override
	public <T extends Entity> int insert(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(bean.getClass());
		String databaseName = metaData.getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String insertQuery = RdbmsQueryBuilder.getInstance().getInsertQuery(metaData);
		Map<String, Object> columnToValueMap = RdbmsHelper.generateColumnToValueMap(bean, metaData);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(bean);
		return pm.executeUpdate(insertQuery, columnToValueMap);
	}

	@Override
	public <T extends Entity> int upsert(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(bean.getClass());
		String databaseName = metaData.getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String upsertQuery = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData);
		Map<String, Object> columnToValueMap = RdbmsHelper.generateColumnToValueMap(bean, metaData);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(bean);
		return pm.executeUpdate(upsertQuery, columnToValueMap);
	}

	@Override
	public <T extends Entity> int upsert(T bean, List<String> columnsToBeUpdatedOnDuplicate)
	        throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(bean.getClass());
		String databaseName = metaData.getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String upsertQuery = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData, columnsToBeUpdatedOnDuplicate);
		Map<String, Object> columnToValueMap = RdbmsHelper.generateColumnToValueMap(bean, metaData);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(bean);
		return pm.executeUpdate(upsertQuery, columnToValueMap);
	}

	@Override
	public <T extends Entity> int update(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(bean.getClass());
		String databaseName = metaData.getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String updateQuery = RdbmsQueryBuilder.getInstance().getUpdateByPkQuery(metaData);
		Map<String, Object> columnToValueMap = RdbmsHelper.generateColumnToValueMap(bean, metaData);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(bean);
		return pm.executeUpdate(updateQuery, columnToValueMap);
	}

	@Override
	public <T extends Entity> int update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		int rowsUpdated = 0;
		String databaseName = metaDataCache.getTableMetaData(clazz).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String shardKeyFieldName = metaDataCache.getShardKeyFieldName(clazz);
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(clazz);
		Map<String, Object> updateColumnToValueMap = RdbmsHelper.generateColumnToValueMap(clazz, updateValuesMap);
		Map<String, Object> criteriaColumnToValueMap = RdbmsHelper.generateColumnToValueMap(clazz, criteria);
		String tableName = metaData.getTableName();
		Map<String, Object> placeHolderToValueMap = new HashMap<String, Object>();
		String updateQuery =
		        RdbmsQueryBuilder.getInstance().getUpdateByCriteriaQuery(tableName, updateColumnToValueMap,
		                criteriaColumnToValueMap, placeHolderToValueMap);
		if (criteria.containsKey(shardKeyFieldName))
		{
			String shardKey = PortKeyUtils.toString(criteria.get(shardKeyFieldName));
			RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardKey);
			return pm.executeUpdate(updateQuery, placeHolderToValueMap);
		}
		else
		{
			List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
			for (RdbmsPersistenceManager pm : persistenceManagersList)
			{
				rowsUpdated += pm.executeUpdate(updateQuery, placeHolderToValueMap);
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

	private SqlQuery getSqlQueryFromUpdateQuery(UpdateQuery query)
	{
		SqlQuery sqlQuery = new SqlQuery();
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(query.getClazz());
		String tableName = metaData.getTableName();
		Map<String, Object> criteriaFieldNameToValueMap = query.getCriteriaFieldNameToValueMap();
		Map<String, Object> criteriaColumnToValueMap =
		        RdbmsHelper.generateColumnToValueMap(query.getClazz(), criteriaFieldNameToValueMap);
		Map<String, Object> updateFieldNameToValueMap = query.getUpdateFieldNameToValueMap();
		Map<String, Object> updateColumnToValueMap =
		        RdbmsHelper.generateColumnToValueMap(query.getClazz(), updateFieldNameToValueMap);
		Map<String, Object> placeHolderToValueMap = new HashMap<String, Object>();
		String queryString =
		        RdbmsQueryBuilder.getInstance().getUpdateByCriteriaQuery(tableName, updateColumnToValueMap,
		                criteriaColumnToValueMap, placeHolderToValueMap);
		sqlQuery.setQuery(queryString);
		sqlQuery.setColumnToValueMap(placeHolderToValueMap);
		return sqlQuery;
	}

	@Override
	public <T extends Entity> int insert(List<T> beans) throws QueryExecutionException
	{
		List<SqlQuery> sqlQueryList = new ArrayList<SqlQuery>();
		Set<String> databases = new HashSet<String>();
		String databaseName = null;
		Set<String> shardIds = new HashSet<String>();
		String shardId = null;
		for (T bean : beans)
		{
			String shardKeyFieldName = metaDataCache.getShardKeyFieldName(bean.getClass());
			RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(bean.getClass());
			databaseName = metaData.getDatabaseName();
			databases.add(databaseName);
			if (databases.size() > 1)
			{
				throw new QueryExecutionException("Transactional inserts in multiple databases are not supported.");
			}
			String shardKey = PortKeyUtils.toString(PortKeyUtils.getFieldValueFromBean(bean, shardKeyFieldName));
			shardId = getShardId(PortKeyUtils.toString(shardKey), metaData);
			shardIds.add(shardId);
			if (shardIds.size() > 1)
			{
				throw new QueryExecutionException("Transactional inserts in multiple shards are not supported.");
			}
			SqlQuery sqlQuery = new SqlQuery();
			Map<String, Object> columnToValueMap = RdbmsHelper.generateColumnToValueMap(bean, metaData);
			sqlQuery.setColumnToValueMap(columnToValueMap);
			String insertQuery = RdbmsQueryBuilder.getInstance().getInsertQuery(metaData);;
			sqlQuery.setQuery(insertQuery);
			sqlQuery.setColumnToValueMap(columnToValueMap);
			sqlQueryList.add(sqlQuery);
		}
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardId);
		return pm.executeUpdates(sqlQueryList);
	}

	@Override
	public int update(List<UpdateQuery> queries, boolean failIfNoRowsAreUpdated) throws QueryExecutionException
	{
		List<SqlQuery> sqlQueryList = new ArrayList<SqlQuery>();
		Set<String> databases = new HashSet<String>();
		String databaseName = null;
		Set<String> shardIds = new HashSet<String>();
		String shardId = null;
		for (UpdateQuery query : queries)
		{
			UpdateQuery update = query;
			String shardKeyFieldName = metaDataCache.getShardKeyFieldName(update.getClazz());
			RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(update.getClazz());
			Map<String, Object> criteria = update.getCriteriaFieldNameToValueMap();
			if (criteria.containsKey(shardKeyFieldName))
			{
				databaseName = metaData.getDatabaseName();
				databases.add(databaseName);
				if (databases.size() > 1)
				{
					throw new QueryExecutionException(
					        "Updates in multiple databases are not supported in single transaction.");
				}
				shardId = getShardId(PortKeyUtils.toString(criteria.get(shardKeyFieldName)), metaData);
				shardIds.add(shardId);
				if (shardIds.size() > 1)
				{
					throw new QueryExecutionException(
					        "Updates on multiple shards are not supported in single transaction.");
				}

				SqlQuery sqlQuery = getSqlQueryFromUpdateQuery(update);
				sqlQueryList.add(sqlQuery);
			}
			else
			{
				throw new QueryExecutionException(
				        "Atomic update queries with no shard identifier field in criteria are not supported");
			}
		}
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardId);
		return pm.executeUpdates(sqlQueryList, failIfNoRowsAreUpdated);
	}

	@Override
	public int update(List<UpdateQuery> queries) throws QueryExecutionException
	{
		return update(queries, false);
	}

	@Override
	public <T extends Entity> int delete(Class<T> clazz, Map<String, Object> criteria) throws QueryExecutionException
	{
		int rowsDeleted = 0;
		String databaseName = metaDataCache.getTableMetaData(clazz).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(clazz);
		String tableName = metaData.getTableName();
		String shardKeyFieldName = metaDataCache.getShardKeyFieldName(clazz);
		Map<String, Object> deleteCriteriaColumnToValueMap = RdbmsHelper.generateColumnToValueMap(clazz, criteria);
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
		String databaseName = metaDataCache.getTableMetaData(clazz).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String shardKeyFieldName = metaDataCache.getShardKeyFieldName(clazz);
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(clazz);
		String tableName = metaData.getTableName();
		Map<String, Object> criteriaColumnToValueMap = RdbmsHelper.generateColumnToValueMap(clazz, criteria);
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
		String databaseName = metaDataCache.getTableMetaData(clazz).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String shardKeyFieldName = metaDataCache.getShardKeyFieldName(clazz);
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getTableMetaData(clazz);
		String tableName = metaData.getTableName();
		List<String> columnsInSelect = RdbmsHelper.generateColumnsListFromFieldNamesList(metaData, fieldsInSelect);
		Map<String, Object> criteriaColumnToValueMap = RdbmsHelper.generateColumnToValueMap(clazz, criteria);
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
		stopwatch.start("Entry: getBySql");
		List<T> result = new ArrayList<T>();
		String databaseName = metaDataCache.getTableMetaData(clazz).getDatabaseName();
		stopwatch.lap("Fetched db name");
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		stopwatch.lap("Fetched db config");
		String shardKeyFieldName = metaDataCache.getShardKeyFieldName(clazz);
		stopwatch.lap("Fetched shard key field name");
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		stopwatch.lap("Fetched mapper instance");
		if (criteria != null && criteria.containsKey(shardKeyFieldName))
		{
			String shardKey = PortKeyUtils.toString(criteria.get(shardKeyFieldName));
			stopwatch.lap("Criteria contains shardkey, fetched shard key");
			RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardKey);
			stopwatch.lap("Fetched persistence manager");
			result = pm.executeQuery(readMaster, sql, criteria, mapper);
			stopwatch.lap("Received result");
		}
		else
		{
			stopwatch.lap("Criteria doesn't contain shardkey");
			List<RdbmsPersistenceManager> persistenceManagersList = databaseConfig.getAllPersistenceManagers();
			stopwatch.lap("Fetched all persistence managers");
			for (RdbmsPersistenceManager pm : persistenceManagersList)
			{
				stopwatch.lap("Fetching data from " + pm);
				result.addAll(pm.executeQuery(readMaster, sql, criteria, mapper));
				stopwatch.lap("Added data to result");
			}
		}
		stopwatch.lap("Returning result");
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

	@Override
	public <T extends Entity> RdbmsTransactionManager getTransactionManager(T bean) throws ShardNotAvailableException
	{
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(bean.getClass());
		String databaseName = metaData.getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		return databaseConfig.getTransactionManager(bean);
	}

	@Override
	public <T extends JoinEntity> List<T> getByJoinCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		return getByJoinCriteria(clazz, attributeNames, criteria, false);
	}

	@Override
	public <T extends JoinEntity> List<T> getByJoinCriteria(Class<T> clazz, List<String> fieldNames,
	        Map<String, Object> criteriaFieldToValueMap, boolean readMaster) throws QueryExecutionException
	{
		RdbmsJoinMetaData metaData = metaDataCache.getJoinMetaData(clazz);
		String databaseName = metaData.getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		String shardKeyFieldName = metaDataCache.getJoinShardKeyFieldName(clazz);
		Map<String, Object> criteriaColumnToValueMap =
		        RdbmsHelper.generateJoinColumnToValueMap(clazz, criteriaFieldToValueMap);
		List<String> criteriaColumnList = new ArrayList<String>(criteriaColumnToValueMap.keySet());
		String getQuery =
		        RdbmsQueryBuilder.getInstance().getGetByJoinCriteriaQuery(metaData, fieldNames, criteriaColumnList);
		RdbmsJoinMapper<T> mapper = RdbmsJoinMapper.getInstance(clazz);

		if (criteriaFieldToValueMap != null && criteriaFieldToValueMap.containsKey(shardKeyFieldName))
		{
			String shardKey = PortKeyUtils.toString(criteriaFieldToValueMap.get(shardKeyFieldName));
			RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardKey);
			return pm.executeQuery(readMaster, getQuery, criteriaColumnToValueMap, mapper);
		}
		else
		{
			throw new QueryNotSupportedException("Shard key value unknown, failed to identify shard to execute query.");
		}
	}
}
