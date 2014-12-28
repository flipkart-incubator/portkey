/**
 * 
 */
package com.flipkart.portkey.rdbms.persistence;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.InvalidAnnotationException;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.common.helper.PortKeyHelper;
import com.flipkart.portkey.common.persistence.PersistenceManager;
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
	DataSource master;
	List<DataSource> slaves;

	public RdbmsPersistenceManager(RdbmsPersistenceManagerConfig config)
	{
		this.master = config.getMaster();
		this.slaves = config.getSlaves();
	}

	public ShardStatus healthCheck()
	{
		logger.info("health checking for rdbms shard master=" + master);
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
		logger.info("checking if master is available for write");
		try
		{
			new JdbcTemplate(master).execute("SELECT 1 FROM DUAL");
		}
		catch (DataAccessException e)
		{
			logger.info("exception while trying to execute query on master" + master);
			return false;
		}
		logger.info("master is available for write");
		return true;
	}

	/**
	 * @return
	 */
	private boolean isAvailableForRead()
	{
		logger.info("checking if any slave is available");
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
			logger.info("slave is up" + slave);
			return true;
		}
		logger.info("no slave is up");
		return false;
	}

	/**
	 * @param bean
	 * @param metaData
	 * @return
	 */
	private Map<String, Object> generateAttributeToValueMap(Entity obj, RdbmsTableMetaData tableMetaData)
	{
		List<Field> fieldList = tableMetaData.getFieldList();
		Map<String, Object> mapValues = new HashMap<String, Object>();
		for (Field field : fieldList)
		{
			RdbmsField rdbmsField = field.getAnnotation(RdbmsField.class);

			// TODO: write this mapper according to the guidelines given by Ashish
			if (rdbmsField != null)
			{
				Object value;
				try
				{
					if (!field.isAccessible())
					{
						field.setAccessible(true);
					}
					value = field.get(obj);
				}
				catch (IllegalArgumentException e)
				{
					logger.info(e);
					value = null; // TODO: check if this null assignment is correct expected behavior
				}
				catch (IllegalAccessException e)
				{
					logger.info(e);
					value = null;// TODO: check if this null assignment is correct expected behavior
				}
				if (value == null)
				{
					mapValues.put(field.getName(), null);
				}
				else if (value.getClass().isEnum())
				{
					mapValues.put(field.getName(), value.toString());
				}
				else if (rdbmsField.isJson() || rdbmsField.isJsonList())
				{
					mapValues.put(field.getName(), PortKeyHelper.toJsonString(value));
				}
				else
				{
					mapValues.put(field.getName(), value);
				}
			}
		}

		return mapValues;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#insert(com.flipkart.portkey.common.entity.Entity)
	 */
	public <T extends Entity> int insert(T bean) throws ShardNotAvailableException, InvalidAnnotationException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(bean.getClass());
		String insertQuery = RdbmsQueryBuilder.getInstance().getInsertQuery(metaData);
		Map<String, Object> attributeToValueMap = generateAttributeToValueMap(bean, metaData);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		try
		{
			return temp.update(insertQuery, attributeToValueMap);
		}
		catch (DataAccessException e)
		{
			throw new ShardNotAvailableException("Exception while trying to update bean:" + bean + "\n" + e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#update(com.flipkart.portkey.common.entity.Entity)
	 */
	public <T extends Entity> int update(T bean) throws ShardNotAvailableException, InvalidAnnotationException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(bean.getClass());
		String updateQuery = RdbmsQueryBuilder.getInstance().getUpdateByPkQuery(metaData);
		Map<String, Object> attributeToValueMap = generateAttributeToValueMap(bean, metaData);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		try
		{
			return temp.update(updateQuery, attributeToValueMap);
		}
		catch (DataAccessException e)
		{
			throw new ShardNotAvailableException("Exception while trying to update bean:" + bean + "\n" + e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#update(java.lang.Class, java.util.Map,
	 * java.util.Map)
	 */
	public <T extends Entity> int update(Class<T> clazz, Map<String, Object> updateAttributesToValuesMap,
	        Map<String, Object> criteria) throws ShardNotAvailableException, InvalidAnnotationException
	{
		RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		String updateQuery =
		        RdbmsQueryBuilder.getInstance().getUpdateByCriteriaQuery(tableMetaData, updateAttributesToValuesMap,
		                criteria);
		JdbcTemplate temp = new JdbcTemplate(master);
		try
		{
			return temp.update(updateQuery);
		}
		catch (DataAccessException e)
		{
			throw new ShardNotAvailableException("Exception while trying to execute update query:" + updateQuery + "\n"
			        + e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#delete(java.lang.Class, java.util.Map)
	 */
	public <T extends Entity> int delete(Class<T> clazz, Map<String, Object> criteria)
	        throws ShardNotAvailableException, InvalidAnnotationException
	{
		RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		String deleteQuery = RdbmsQueryBuilder.getInstance().getDeleteByCriteriaQuery(tableMetaData, criteria);
		JdbcTemplate temp = new JdbcTemplate(master);
		try
		{
			return temp.update(deleteQuery);
		}
		catch (DataAccessException e)
		{
			throw new ShardNotAvailableException("Exception while trying to execute delete query:" + deleteQuery + "\n"
			        + e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.Map)
	 */
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria, boolean readMaster)
	        throws ShardNotAvailableException, InvalidAnnotationException
	{
		RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		String updateQuery = RdbmsQueryBuilder.getInstance().getGetByCriteriaQuery(tableMetaData, criteria);
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		JdbcTemplate temp;
		List<T> result;
		if (readMaster)
		{
			temp = new JdbcTemplate(master);
			try
			{
				result = temp.query(updateQuery, mapper);
			}
			catch (DataAccessException e)
			{
				throw new ShardNotAvailableException("Exception while trying to execute get query:" + updateQuery
				        + " on master:" + master + "\n" + e);
			}
			return result;
		}
		else
		{
			for (DataSource slave : slaves)
			{
				temp = new JdbcTemplate(slave);
				try
				{
					result = temp.query(updateQuery, mapper);
				}
				catch (DataAccessException e)
				{
					// TODO: catch specific exception
					logger.info("Exception while trying to execute get query:" + updateQuery + " on slave:" + slave
					        + "\n" + e);
					continue;
				}
				return result;
			}
			temp = new JdbcTemplate(master);
			try
			{
				result = temp.query(updateQuery, mapper);
			}
			catch (DataAccessException e)
			{
				logger.info("Exception while trying to execute get query:" + updateQuery + " on master:" + master
				        + "\n" + e);
				throw new ShardNotAvailableException("Exception while executing query:" + updateQuery
				        + "\nShard is down\n" + e);
			}
			return result;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.Map,
	 * boolean)
	 */
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws ShardNotAvailableException, InvalidAnnotationException
	{
		return getByCriteria(clazz, criteria, false);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.List,
	 * java.util.Map)
	 */
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria, boolean readMaster) throws ShardNotAvailableException,
	        InvalidAnnotationException
	{
		RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		String getQuery =
		        RdbmsQueryBuilder.getInstance().getGetByCriteriaQuery(tableMetaData, attributeNames, criteria);
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		JdbcTemplate temp;
		List<T> result;
		if (readMaster)
		{
			temp = new JdbcTemplate(master);
			try
			{
				result = temp.query(getQuery, mapper);
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
				temp = new JdbcTemplate(slave);
				try
				{
					result = temp.query(getQuery, mapper);
				}
				catch (Exception e)
				{
					logger.info("Exception while trying to execute get query:" + getQuery + " on slave:" + slave + "\n"
					        + e);
					continue;
				}
				return result;
			}
			temp = new JdbcTemplate(master);
			try
			{
				result = temp.query(getQuery, mapper);
			}
			catch (Exception e)
			{
				logger.info("Exception while trying to execute get query:" + getQuery + " on master:" + master + "\n"
				        + e);
				throw new ShardNotAvailableException("Exception while executing query:" + getQuery
				        + "\nShard is down\n" + e);
			}
			return result;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.List,
	 * java.util.Map, boolean)
	 */
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
		JdbcTemplate temp;
		List<T> result;
		if (readMaster)
		{
			temp = new JdbcTemplate(master);
			try
			{
				result = temp.query(sql, mapper);
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
				temp = new JdbcTemplate(slave);
				try
				{
					result = temp.query(sql, mapper);
				}
				catch (Exception e)
				{
					logger.info("Exception while trying to execute sql:" + sql + " on slave:" + slave + "\n" + e);
					continue;
				}
				return result;
			}
			temp = new JdbcTemplate(master);
			try
			{
				result = temp.query(sql, mapper);
			}
			catch (Exception e)
			{
				logger.info("Exception while trying to execute sql:" + sql + " on master:" + master + "\n" + e);
				throw new ShardNotAvailableException("Exception while trying to execute sql:" + sql + " on master:"
				        + master + "\n" + e);
			}
			return result;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.Class, java.lang.String,
	 * java.util.Map, boolean)
	 */
	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws ShardNotAvailableException
	{
		return getBySql(clazz, sql, criteria, false);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.String, java.util.Map)
	 */
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria, boolean readMaster)
	        throws ShardNotAvailableException
	{
		JdbcTemplate temp;
		List<Map<String, Object>> result;
		if (readMaster)
		{
			temp = new JdbcTemplate(master);
			try
			{
				result = temp.queryForList(sql);
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
				temp = new JdbcTemplate(slave);
				try
				{
					result = temp.queryForList(sql);
				}
				catch (Exception e)
				{
					logger.info("Exception while trying to execute sql:" + sql + " on slave:" + slave + "\n" + e);
					continue;
				}
				return result;
			}
			temp = new JdbcTemplate(master);
			try
			{
				result = temp.queryForList(sql);
			}
			catch (Exception e)
			{
				logger.info("Exception while trying to execute sql:" + sql + " on master:" + master + "\n" + e);
				throw new ShardNotAvailableException("Exception while trying to execute sql:" + sql + " on master:"
				        + master + "\n" + e);
			}
			return result;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.String, java.util.Map,
	 * boolean)
	 */
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria)
	        throws ShardNotAvailableException
	{
		return getBySql(sql, criteria, false);
	}
}
