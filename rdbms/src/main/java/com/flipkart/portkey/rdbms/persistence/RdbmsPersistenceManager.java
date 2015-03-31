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
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
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
		this.slaves = config.getSlaves();
	}

	public ShardStatus healthCheck()
	{
		logger.debug("health checking for rdbms shard master=" + master);
		if (isAvailableForWrite())
			return ShardStatus.AVAILABLE_FOR_WRITE;
		else if (isAvailableForRead())
			return ShardStatus.AVAILABLE_FOR_READ;
		else
			return ShardStatus.UNAVAILABLE;
	}

	/**
	 * @return
	 */
	private boolean isAvailableForWrite()
	{
		try
		{
			logger.debug("checking if master is available for write");
			new JdbcTemplate(master).execute("SELECT 1 FROM DUAL");
		}
		catch (DataAccessException e)
		{
			logger.info("exception while trying to execute query on master" + master);
			return false;
		}
		logger.debug("master is available for write");
		return true;
	}

	/**
	 * @return
	 */
	private boolean isAvailableForRead()
	{
		logger.debug("checking if any slave is available");
		for (DataSource slave : slaves)
		{
			try
			{
				new JdbcTemplate(slave).execute("SELECT 1 FROM DUAL");
			}
			catch (DataAccessException e)
			{
				logger.info("exception while trying to execute query on slave" + slave);
				continue;
			}
			logger.debug("slave is up" + slave);
			return true;
		}
		logger.debug("no slave is up");
		return false;
	}

	/**
	 * @param bean
	 * @param metaData
	 * @return
	 */
	private Map<String, Object> generateRdbmsColumnToValueMap(Entity obj, RdbmsTableMetaData tableMetaData)
	{
		List<Field> fieldList = tableMetaData.getFieldList();
		Map<String, Object> mapValues = new HashMap<String, Object>();
		for (Field field : fieldList)
		{
			RdbmsField rdbmsField = field.getAnnotation(RdbmsField.class);
			if (rdbmsField != null)
			{
				Object value;
				value = RdbmsMapper.get(obj, field.getName());
				if (value instanceof Enum)
				{
					String enumStr = PortKeyUtils.enumToString((Enum) value);
					mapValues.put(rdbmsField.columnName(), enumStr);
				}
				else if (value instanceof List)
				{
					String listStr = PortKeyUtils.listToString((List) value);
					mapValues.put(rdbmsField.columnName(), listStr);
				}
				else
				{
					mapValues.put(rdbmsField.columnName(), value);
				}
			}
		}
		return mapValues;
	}

	private Map<String, Object> generateRdbmsColumnToValueMap(Map<String, Object> fieldNamesToValuesMap,
	        RdbmsTableMetaData tableMetaData)
	{
		Map<String, Object> mapValues = new HashMap<String, Object>();
		for (String fieldName : fieldNamesToValuesMap.keySet())
		{
			String rdbmsColumn = tableMetaData.getRdbmsColumnFromFieldName(fieldName);
			Object value = fieldNamesToValuesMap.get(fieldName);
			if (value instanceof Enum)
			{
				String enumStr = PortKeyUtils.enumToString((Enum) value);
				mapValues.put(rdbmsColumn, enumStr);
			}
			else if (value instanceof List)
			{
				String listStr = PortKeyUtils.listToString((List) value);
				mapValues.put(rdbmsColumn, listStr);
			}
			else
			{
				mapValues.put(rdbmsColumn, value);
			}
		}
		return mapValues;
	}

	private Map<String, Object> getRdbmsFieldsMap(Map<String, Object> attributesMap, RdbmsTableMetaData tableMetaData)
	{
		Map<String, Object> rdbmsFieldsMap = new HashMap<String, Object>();
		for (String key : attributesMap.keySet())
		{
			String columnName = tableMetaData.getRdbmsColumnFromFieldName(key);
			Object value = attributesMap.get(key);
			if (value instanceof Enum)
			{
				String enumStr = PortKeyUtils.enumToString((Enum) value);
				rdbmsFieldsMap.put(columnName, enumStr);
			}
			else if (value instanceof List)
			{
				String listStr = PortKeyUtils.listToString((List) value);
				rdbmsFieldsMap.put(columnName, listStr);
			}
			else
			{
				rdbmsFieldsMap.put(columnName, attributesMap.get(key));
			}
		}
		return rdbmsFieldsMap;
	}

	public <T extends Entity> int insert(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(bean.getClass());
		String insertQuery = RdbmsQueryBuilder.getInstance().getInsertQuery(metaData);
		Map<String, Object> rdbmsColumnToValueMap = generateRdbmsColumnToValueMap(bean, metaData);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		try
		{
			return temp.update(insertQuery, rdbmsColumnToValueMap);
		}
		catch (DataAccessException e)
		{
			logger.error("Exception while trying to update bean:" + bean + "\nnamed parameter jdbc template:" + temp, e);
			throw new QueryExecutionException("Exception while trying to insert bean:" + bean + ", exception:"
			        + e.toString());
		}
	}

	@Override
	public <T extends Entity> int upsert(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(bean.getClass());
		String insertQuery = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData);
		Map<String, Object> rdbmsColumnToValueMap = generateRdbmsColumnToValueMap(bean, metaData);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		try
		{
			return temp.update(insertQuery, rdbmsColumnToValueMap);
		}
		catch (DataAccessException e)
		{
			logger.warn("Exception while trying to update bean:" + bean + ", exception:" + e.toString());
			throw new QueryExecutionException("Exception while trying to upsert bean:" + bean + ", exception:"
			        + e.toString());
		}
	}

	@Override
	public <T extends Entity> int upsert(T bean, List<String> columnsToBeUpdatedOnDuplicate)
	        throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(bean.getClass());
		String insertQuery = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData, columnsToBeUpdatedOnDuplicate);
		Map<String, Object> rdbmsColumnToValueMap = generateRdbmsColumnToValueMap(bean, metaData);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		try
		{
			return temp.update(insertQuery, rdbmsColumnToValueMap);
		}
		catch (DataAccessException e)
		{
			logger.warn("Exception while trying to update bean:" + bean + ", exception:" + e.toString());
			throw new QueryExecutionException("Exception while trying to upsert bean:" + bean + ", exception:"
			        + e.toString());
		}
	}

	public <T extends Entity> int update(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(bean.getClass());
		String updateQuery = RdbmsQueryBuilder.getInstance().getUpdateByPkQuery(metaData);
		Map<String, Object> rdbmsColumnToValueMap = generateRdbmsColumnToValueMap(bean, metaData);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		try
		{
			return temp.update(updateQuery, rdbmsColumnToValueMap);
		}
		catch (DataAccessException e)
		{
			logger.warn("Exception while executing update on bean" + bean + ", exception:" + e.toString());
			throw new QueryExecutionException("Exception while trying to update bean:" + bean + ", exception:"
			        + e.toString());
		}
	}

	public <T extends Entity> int update(Class<T> clazz, Map<String, Object> updateAttributesToValuesMap,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		Map<String, Object> rdbmsColumnToValueMap =
		        generateRdbmsColumnToValueMap(updateAttributesToValuesMap, tableMetaData);
		Map<String, Object> rdbmsCriteria = generateRdbmsColumnToValueMap(criteria, tableMetaData);
		String tableName = tableMetaData.getTableName();
		List<String> rdbmsColumns = new ArrayList<String>(rdbmsColumnToValueMap.keySet());
		List<String> criteriaAttributes = new ArrayList<String>(rdbmsCriteria.keySet());
		String updateQuery =
		        RdbmsQueryBuilder.getInstance().getUpdateByCriteriaQuery(tableName, rdbmsColumns, criteriaAttributes);
		Map<String, Object> namedParameter = PortKeyUtils.mergeMaps(rdbmsColumnToValueMap, rdbmsCriteria);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		try
		{
			return temp.update(updateQuery, namedParameter);
		}
		catch (DataAccessException e)
		{
			throw new QueryExecutionException("Exception while trying to execute update query:" + updateQuery
			        + ", exception:" + e.toString());
		}
	}

	@Override
	@Transactional
	public <T extends Entity> List<Integer> update(List<UpdateQuery> updates) throws QueryExecutionException
	{
		List<Integer> rowsUpdatedList = new ArrayList<Integer>();
		for (UpdateQuery update : updates)
		{
			Class<? extends Entity> clazz = update.getClazz();
			Map<String, Object> updateAttributesToValuesMap = update.getUpdateValuesMap();
			Map<String, Object> criteria = update.getCriteria();
			RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
			Map<String, Object> rdbmsColumnToValueMap =
			        generateRdbmsColumnToValueMap(updateAttributesToValuesMap, tableMetaData);
			Map<String, Object> rdbmsCriteria = generateRdbmsColumnToValueMap(criteria, tableMetaData);
			String tableName = tableMetaData.getTableName();
			List<String> rdbmsColumns = new ArrayList<String>(rdbmsColumnToValueMap.keySet());
			List<String> criteriaAttributes = new ArrayList<String>(rdbmsCriteria.keySet());
			String updateQuery =
			        RdbmsQueryBuilder.getInstance().getUpdateByCriteriaQuery(tableName, rdbmsColumns,
			                criteriaAttributes);
			Map<String, Object> namedParameter = PortKeyUtils.mergeMaps(rdbmsColumnToValueMap, rdbmsCriteria);
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

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#delete(java.lang.Class, java.util.Map)
	 */
	public <T extends Entity> int delete(Class<T> clazz, Map<String, Object> criteria)
	        throws ShardNotAvailableException
	{
		RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		String tableName = tableMetaData.getTableName();
		Map<String, Object> rdbmsCriteria = generateRdbmsColumnToValueMap(criteria, tableMetaData);
		List<String> criteriaAttributes = new ArrayList<String>(rdbmsCriteria.keySet());
		String deleteQuery = RdbmsQueryBuilder.getInstance().getDeleteByCriteriaQuery(tableName, criteriaAttributes);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		try
		{
			return temp.update(deleteQuery, rdbmsCriteria);
		}
		catch (DataAccessException e)
		{
			logger.warn("Exception while trying to execute delete query:" + deleteQuery + ", exception:" + e.toString());
			throw new ShardNotAvailableException("Exception while trying to execute delete query:" + deleteQuery
			        + ", exception:" + e.toString());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.Map)
	 */
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria, boolean readMaster)
	        throws ShardNotAvailableException
	{
		RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		String tableName = tableMetaData.getTableName();
		Map<String, Object> rdbmsCriteria = getRdbmsFieldsMap(criteria, tableMetaData);
		List<String> criteriaAttributes = new ArrayList<String>(rdbmsCriteria.keySet());
		String updateQuery = RdbmsQueryBuilder.getInstance().getGetByCriteriaQuery(tableName, criteriaAttributes);
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		NamedParameterJdbcTemplate temp;
		List<T> result;
		if (readMaster)
		{
			temp = new NamedParameterJdbcTemplate(master);
			try
			{
				result = temp.query(updateQuery, rdbmsCriteria, mapper);
				return result;
			}
			catch (DataAccessException e)
			{
				logger.warn("Exception while trying to execute get query:" + updateQuery + " on master:" + master
				        + ", exception:" + e.toString());
				throw new ShardNotAvailableException("Exception while trying to execute get query:" + updateQuery
				        + " on master:" + master + ", exception:" + e.toString());
			}
		}
		else
		{
			for (DataSource slave : slaves)
			{
				temp = new NamedParameterJdbcTemplate(slave);
				try
				{
					result = temp.query(updateQuery, rdbmsCriteria, mapper);
					return result;
				}
				catch (DataAccessException e)
				{
					logger.warn("Exception while trying to execute get query:" + updateQuery + " on slave:" + slave
					        + ", exception:" + e.toString());
					continue;
				}
			}
			temp = new NamedParameterJdbcTemplate(master);
			try
			{
				result = temp.query(updateQuery, rdbmsCriteria, mapper);
				return result;
			}
			catch (DataAccessException e)
			{
				logger.warn("Exception while trying to execute get query:" + updateQuery + " on master:" + master
				        + ", exception:" + e.toString());
				throw new ShardNotAvailableException("Exception while trying to execute get query:" + updateQuery
				        + " on master:" + master + ", exception:" + e.toString());
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.Map,
	 * boolean)
	 */
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws ShardNotAvailableException
	{
		return getByCriteria(clazz, criteria, false);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.List,
	 * java.util.Map)
	 */
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria, boolean readMaster) throws ShardNotAvailableException
	{
		RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		String tableName = tableMetaData.getTableName();
		Map<String, Object> rdbmsCriteria = getRdbmsFieldsMap(criteria, tableMetaData);
		List<String> criteriaAttributes = new ArrayList<String>(rdbmsCriteria.keySet());
		String getQuery =
		        RdbmsQueryBuilder.getInstance().getGetByCriteriaQuery(tableName, attributeNames, criteriaAttributes);
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		NamedParameterJdbcTemplate temp;
		List<T> result;
		if (readMaster)
		{
			temp = new NamedParameterJdbcTemplate(master);
			try
			{
				result = temp.query(getQuery, rdbmsCriteria, mapper);
			}
			catch (DataAccessException e)
			{
				throw new ShardNotAvailableException("Exception while trying to execute get query:" + getQuery
				        + " on master:" + master + "\n" + e);
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
					result = temp.query(getQuery, rdbmsCriteria, mapper);
				}
				catch (Exception e)
				{
					logger.warn("Exception while trying to execute get query:" + getQuery + " on slave:" + slave, e);
					continue;
				}
				return result;
			}
			temp = new NamedParameterJdbcTemplate(master);
			try
			{
				result = temp.query(getQuery, rdbmsCriteria, mapper);
			}
			catch (Exception e)
			{
				logger.warn("Exception while trying to execute get query:" + getQuery + " on master:" + master, e);
				throw new ShardNotAvailableException("Exception while executing query:" + getQuery + "\nShard is down",
				        e);
			}
			return result;
		}
	}

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws ShardNotAvailableException, InvalidAnnotationException
	{
		return getByCriteria(clazz, attributeNames, criteria, false);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.Class, java.lang.String,
	 * java.util.Map)
	 */
	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria,
	        boolean readMaster) throws ShardNotAvailableException
	{
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		NamedParameterJdbcTemplate temp;
		List<T> result;
		if (readMaster)
		{
			temp = new NamedParameterJdbcTemplate(master);
			try
			{
				result = temp.query(sql, criteria, mapper);
			}
			catch (Exception e)
			{
				throw new ShardNotAvailableException("Exception while trying to execute sql:" + sql + " on master:"
				        + master + "\n" + e);
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
					result = temp.query(sql, criteria, mapper);
				}
				catch (Exception e)
				{
					logger.warn("Exception while trying to execute sql:" + sql + " on slave:" + slave, e);
					continue;
				}
				return result;
			}
			temp = new NamedParameterJdbcTemplate(master);
			try
			{
				result = temp.query(sql, criteria, mapper);
			}
			catch (Exception e)
			{
				logger.warn("Exception while trying to execute sql:" + sql + " on master:" + master, e);
				throw new ShardNotAvailableException("Exception while trying to execute sql:" + sql + " on master:"
				        + master, e);
			}
			return result;
		}
	}

	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws ShardNotAvailableException
	{
		return getBySql(clazz, sql, criteria, false);
	}

	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria, boolean readMaster)
	        throws ShardNotAvailableException
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
				throw new ShardNotAvailableException("Exception while trying to execute sql:" + sql + " on master:"
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
					logger.warn("Exception while trying to execute sql:" + sql + " on slave:" + slave, e);
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
				logger.warn("Exception while trying to execute sql:" + sql + " on master:" + master, e);
				throw new ShardNotAvailableException("Exception while trying to execute sql:" + sql + " on master:"
				        + master, e);
			}
			return result;
		}
	}

	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria)
	        throws ShardNotAvailableException
	{
		return getBySql(sql, criteria, false);
	}
}
