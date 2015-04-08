/**
 * 
 */
package com.flipkart.portkey.rdbms.persistence;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.InvalidAnnotationException;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.persistence.PersistenceManager;
import com.flipkart.portkey.common.persistence.query.UpdateQuery;
import com.flipkart.portkey.common.util.PortKeyUtils;
import com.flipkart.portkey.rdbms.mapper.RdbmsMapper;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;
import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;
import com.flipkart.portkey.rdbms.persistence.config.RdbmsPersistenceManagerConfig;
import com.flipkart.portkey.rdbms.querybuilder.RdbmsQueryBuilder;

/**
 * @author santosh.p
 */
public class RdbmsPersistenceManager implements PersistenceManager
{
	private static final Logger logger = Logger.getLogger(RdbmsPersistenceManager.class);
	private final DataSource master;
	private final List<DataSource> slaves;

	public RdbmsPersistenceManager(RdbmsPersistenceManagerConfig config)
	{
		this.master = config.getMaster();
		if (config.getSlaves() == null)
		{
			this.slaves = new ArrayList<DataSource>();
		}
		else
		{
			this.slaves = config.getSlaves();
		}
	}

	public ShardStatus healthCheck()
	{
		logger.debug("Running health check for rdbms shard, master=" + master);
		if (isAvailableForWrite())
			return ShardStatus.AVAILABLE_FOR_WRITE;
		else if (isAvailableForRead())
			return ShardStatus.AVAILABLE_FOR_READ;
		else
			return ShardStatus.UNAVAILABLE;
	}

	private boolean isAvailableForWrite()
	{
		try
		{
			logger.debug("Checking if master is up");
			new JdbcTemplate(master).execute("SELECT 1 FROM DUAL");
		}
		catch (DataAccessException e)
		{
			logger.info("Exception while trying to execute query on master " + master, e);
			return false;
		}
		logger.debug("Master is up");
		return true;
	}

	private boolean isAvailableForRead()
	{
		logger.debug("Checking if any slave up");
		for (DataSource slave : slaves)
		{
			try
			{
				new JdbcTemplate(slave).execute("SELECT 1 FROM DUAL");
			}
			catch (DataAccessException e)
			{
				logger.info("Exception while trying to execute query on slave" + slave, e);
				continue;
			}
			logger.debug("Slave " + slave + " is up");
			return true;
		}
		logger.debug("No slave is up");
		return false;
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

	private <T extends Entity> Map<String, Object> generateColumnToValueMap(Class<T> clazz,
	        Map<String, Object> fieldNameToValueMap, RdbmsTableMetaData metaData)
	{
		Map<String, Object> columnToValueMap = new HashMap<String, Object>();
		for (String fieldName : fieldNameToValueMap.keySet())
		{
			String columnName = metaData.getColumnNameFromFieldName(fieldName);
			Object valueBeforeSerialization = fieldNameToValueMap.get(fieldName);
			Object value = RdbmsMapper.get(clazz, fieldName, valueBeforeSerialization);
			columnToValueMap.put(columnName, value);
		}
		return columnToValueMap;
	}

	private int executeUpdate(NamedParameterJdbcTemplate temp, String query, Map<String, Object> parameterMap)
	        throws QueryExecutionException
	{
		try
		{
			return temp.update(query, parameterMap);
		}
		catch (DataAccessException e)
		{
			throw new QueryExecutionException("Exception while trying to execute query, query=" + query
			        + ", parameter map=" + parameterMap);
		}
	}

	public <T extends Entity> int insert(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(bean.getClass());
		String insertQuery = RdbmsQueryBuilder.getInstance().getInsertQuery(metaData);
		Map<String, Object> columnToValueMap = generateColumnToValueMap(bean, metaData);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		return executeUpdate(temp, insertQuery, columnToValueMap);
	}

	@Override
	public <T extends Entity> int upsert(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(bean.getClass());
		String insertQuery = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData);
		Map<String, Object> columnToValueMap = generateColumnToValueMap(bean, metaData);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		return executeUpdate(temp, insertQuery, columnToValueMap);
	}

	@Override
	public <T extends Entity> int upsert(T bean, List<String> columnsToBeUpdatedOnDuplicate)
	        throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(bean.getClass());
		String insertQuery = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData, columnsToBeUpdatedOnDuplicate);
		Map<String, Object> columnToValueMap = generateColumnToValueMap(bean, metaData);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		return executeUpdate(temp, insertQuery, columnToValueMap);
	}

	public <T extends Entity> int update(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(bean.getClass());
		String updateQuery = RdbmsQueryBuilder.getInstance().getUpdateByPkQuery(metaData);
		Map<String, Object> columnToValueMap = generateColumnToValueMap(bean, metaData);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		return executeUpdate(temp, updateQuery, columnToValueMap);
	}

	public <T extends Entity> int update(Class<T> clazz, Map<String, Object> fieldsToBeUpdated,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		Map<String, Object> updateColumnToValueMap = generateColumnToValueMap(clazz, fieldsToBeUpdated, metaData);
		Map<String, Object> criteriaColumnToValueMap = generateColumnToValueMap(clazz, criteria, metaData);
		String tableName = metaData.getTableName();
		List<String> columnsToBeUpdated = new ArrayList<String>(updateColumnToValueMap.keySet());
		List<String> columnsInCriteria = new ArrayList<String>(criteriaColumnToValueMap.keySet());
		Map<String, Object> columnToValueMap = PortKeyUtils.mergeMaps(updateColumnToValueMap, criteriaColumnToValueMap);
		String updateQuery =
		        RdbmsQueryBuilder.getInstance().getUpdateByCriteriaQuery(tableName, columnsToBeUpdated,
		                columnsInCriteria, columnToValueMap);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		return executeUpdate(temp, updateQuery, columnToValueMap);
	}

	@Override
	@Transactional
	public <T extends Entity> List<Integer> update(List<UpdateQuery> updates) throws QueryExecutionException
	{
		List<Integer> rowsUpdatedList = new ArrayList<Integer>();
		for (UpdateQuery update : updates)
		{
			Class<? extends Entity> clazz = update.getClazz();
			Map<String, Object> updateFieldNameToValuesMap = update.getUpdateFieldNameToValueMap();
			Map<String, Object> criteriaFieldNameToValueMap = update.getCriteriaFieldNameToValueMap();
			RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
			Map<String, Object> updateColumnToValueMap =
			        generateColumnToValueMap(clazz, updateFieldNameToValuesMap, tableMetaData);
			Map<String, Object> criteriaColumnToValueMap =
			        generateColumnToValueMap(clazz, criteriaFieldNameToValueMap, tableMetaData);
			String tableName = tableMetaData.getTableName();
			List<String> columnsToBeUpdated = new ArrayList<String>(updateColumnToValueMap.keySet());
			List<String> criteriaAttributes = new ArrayList<String>(criteriaColumnToValueMap.keySet());
			String updateQuery =
			        RdbmsQueryBuilder.getInstance().getUpdateByCriteriaQuery(tableName, columnsToBeUpdated,
			                criteriaAttributes, updateColumnToValueMap);
			Map<String, Object> namedParameter =
			        PortKeyUtils.mergeMaps(updateColumnToValueMap, criteriaColumnToValueMap);
			NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
			try
			{
				int rowsUpdated = temp.update(updateQuery, namedParameter);
				rowsUpdatedList.add(rowsUpdated);
			}
			catch (DataAccessException e)
			{
				throw new QueryExecutionException("Exception while trying to execute update query:" + updateQuery
				        + ", exception:" + e.toString());
			}
		}
		return rowsUpdatedList;
	}

	@Override
	public <T extends Entity> int delete(Class<T> clazz, Map<String, Object> criteria) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		String tableName = metaData.getTableName();
		Map<String, Object> deleteCriteriaColumnToValueMap = generateColumnToValueMap(clazz, criteria, metaData);
		String deleteQuery =
		        RdbmsQueryBuilder.getInstance().getDeleteByCriteriaQuery(tableName, deleteCriteriaColumnToValueMap);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		return executeUpdate(temp, deleteQuery, deleteCriteriaColumnToValueMap);
	}

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria, boolean readMaster)
	        throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		String tableName = metaData.getTableName();
		Map<String, Object> criteriaColumnToValueMap = generateColumnToValueMap(clazz, criteria, metaData);
		String updateQuery = RdbmsQueryBuilder.getInstance().getGetByCriteriaQuery(tableName, criteriaColumnToValueMap);
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		NamedParameterJdbcTemplate temp;
		List<T> result;
		if (readMaster)
		{
			temp = new NamedParameterJdbcTemplate(master);
			try
			{
				result = temp.query(updateQuery, criteriaColumnToValueMap, mapper);
				return result;
			}
			catch (DataAccessException e)
			{
				throw new QueryExecutionException("Exception while trying to execute get query " + updateQuery
				        + " on master " + master, e);
			}
		}
		else
		{
			for (DataSource slave : slaves)
			{
				temp = new NamedParameterJdbcTemplate(slave);
				try
				{
					result = temp.query(updateQuery, criteriaColumnToValueMap, mapper);
					return result;
				}
				catch (DataAccessException e)
				{
					continue;
				}
			}
			temp = new NamedParameterJdbcTemplate(master);
			try
			{
				result = temp.query(updateQuery, criteriaColumnToValueMap, mapper);
				return result;
			}
			catch (DataAccessException e)
			{
				throw new QueryExecutionException("Exception while trying to execute get query " + updateQuery
				        + " on master " + master, e);
			}
		}
	}

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		return getByCriteria(clazz, criteria, false);
	}

	private <T extends Entity> List<T> executeQuery(DataSource dataSource, String query,
	        Map<String, Object> columnToValueMap, RdbmsMapper<T> mapper) throws QueryExecutionException
	{
		List<T> result;
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(dataSource);
		try
		{
			result = temp.query(query, columnToValueMap, mapper);
		}
		catch (DataAccessException e)
		{
			throw new QueryExecutionException("Exception while trying to execute get query " + query + " on "
			        + dataSource, e);
		}
		return result;
	}

	private <T extends Entity> List<T> executeQuery(boolean readMaster, String query,
	        Map<String, Object> columnToValueMap, RdbmsMapper<T> mapper) throws QueryExecutionException
	{
		if (readMaster)
		{
			return executeQuery(master, query, columnToValueMap, mapper);
		}
		else
		{
			for (DataSource slave : slaves)
			{
				try
				{
					return executeQuery(slave, query, columnToValueMap, mapper);
				}
				catch (Exception e)
				{
					logger.warn("Exception while trying to execute get query " + query + " on slave " + slave, e);
					continue;
				}
			}
			return executeQuery(master, query, columnToValueMap, mapper);
		}

	}

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> fieldsInSelect,
	        Map<String, Object> criteria, boolean readMaster) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		String tableName = metaData.getTableName();
		List<String> columnsInSelect = generateColumnsListFromFieldNamesList(metaData, fieldsInSelect);
		Map<String, Object> criteriaColumnToValueMap = generateColumnToValueMap(clazz, criteria, metaData);
		String getQuery =
		        RdbmsQueryBuilder.getInstance().getGetByCriteriaQuery(tableName, columnsInSelect,
		                criteriaColumnToValueMap);
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		return executeQuery(readMaster, getQuery, criteriaColumnToValueMap, mapper);
	}

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws QueryExecutionException, InvalidAnnotationException
	{
		return getByCriteria(clazz, attributeNames, criteria, false);
	}

	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria,
	        boolean readMaster) throws QueryExecutionException
	{
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		return executeQuery(readMaster, sql, criteria, mapper);
	}

	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		return getBySql(clazz, sql, criteria, false);
	}

	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria, boolean readMaster)
	        throws QueryExecutionException
	{
		NamedParameterJdbcTemplate temp;
		List<Map<String, Object>> result;
		if (readMaster)
		{
			temp = new NamedParameterJdbcTemplate(master);
			try
			{
				result = temp.queryForList(sql, criteria);
			}
			catch (Exception e)
			{
				throw new QueryExecutionException("Exception while trying to execute sql query" + sql + " on master "
				        + master, e);
			}
			return result;
		}
		else
		{
			for (DataSource slave : slaves)
			{
				temp = new NamedParameterJdbcTemplate(slave);
				try
				{
					result = temp.queryForList(sql, criteria);
				}
				catch (Exception e)
				{
					logger.warn("Exception while trying to execute sql query" + sql + " on slave " + slave, e);
					continue;
				}
				return result;
			}
			temp = new NamedParameterJdbcTemplate(master);
			try
			{
				result = temp.queryForList(sql, criteria);
			}
			catch (Exception e)
			{
				throw new QueryExecutionException("Exception while trying to execute sql query" + sql + " on master "
				        + master, e);
			}
			return result;
		}
	}

	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria) throws QueryExecutionException
	{
		return getBySql(sql, criteria, false);
	}

	@Override
	public int updateBySql(String sql, Map<String, Object> criteria) throws QueryExecutionException
	{
		NamedParameterJdbcTemplate temp;
		temp = new NamedParameterJdbcTemplate(master);
		try
		{
			return temp.update(sql, criteria);
		}
		catch (Exception e)
		{
			throw new QueryExecutionException("Exception while trying to execute sql query " + sql + " on master "
			        + master, e);
		}
	}
}
