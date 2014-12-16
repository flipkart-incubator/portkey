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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.PortKeyException;
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
			new JdbcTemplate(master).execute("SELECT 1 FROM DUAL");
		}
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	/**
	 * @return
	 */
	private boolean isAvailableForRead()
	{
		for (DataSource slave : slaves)
		{
			try
			{
				new JdbcTemplate(slave).execute("SELECT 1 FROM DUAL");
			}
			catch (Exception e)
			{
				continue;
			}
			return true;
		}
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

			if (rdbmsField != null)
			{
				Object value;
				try
				{
					value = field.get(obj);
				}
				catch (IllegalArgumentException e)
				{
					logger.info(e);
					value = null;
				}
				catch (IllegalAccessException e)
				{
					logger.info(e);
					value = null;
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
	public int insert(Entity bean)
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getMetaData(bean.getClass());
		String insertQuery = RdbmsQueryBuilder.getInstance().getInsertQuery(metaData);
		Map<String, Object> attributeToValueMap = generateAttributeToValueMap(bean, metaData);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		return temp.update(insertQuery, attributeToValueMap);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#update(com.flipkart.portkey.common.entity.Entity)
	 */
	public int update(Entity bean)
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getMetaData(bean.getClass());
		String updateQuery = RdbmsQueryBuilder.getInstance().getUpdateByPkQuery(metaData);
		Map<String, Object> attributeToValueMap = generateAttributeToValueMap(bean, metaData);
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		return temp.update(updateQuery, attributeToValueMap);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#update(java.lang.Class, java.util.Map,
	 * java.util.Map)
	 */
	public int update(Class<? extends Entity> clazz, Map<String, Object> updateAttributesToValuesMap,
	        Map<String, Object> criteria)
	{
		RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getMetaData(clazz);
		String updateQuery =
		        RdbmsQueryBuilder.getInstance().getUpdateByCriteriaQuery(tableMetaData, updateAttributesToValuesMap,
		                criteria);
		JdbcTemplate temp = new JdbcTemplate(master);
		return temp.update(updateQuery);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#delete(java.lang.Class, java.util.Map)
	 */
	public int delete(Class<? extends Entity> clazz, Map<String, Object> criteria)
	{
		RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getMetaData(clazz);
		String deleteQuery = RdbmsQueryBuilder.getInstance().getDeleteByCriteriaQuery(tableMetaData, criteria);
		JdbcTemplate temp = new JdbcTemplate(master);
		return temp.update(deleteQuery);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.Map)
	 */
	public List<? extends Entity> getByCriteria(Class<? extends Entity> clazz, Map<String, Object> criteria,
	        boolean readMaster) throws PortKeyException
	{
		RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getMetaData(clazz);
		String updateQuery = RdbmsQueryBuilder.getInstance().getGetByCriteriaQuery(tableMetaData, criteria);
		RdbmsMapper<? extends Entity> mapper = RdbmsMapper.getInstance(clazz);
		JdbcTemplate temp;
		List<? extends Entity> result;
		if (readMaster)
		{
			temp = new JdbcTemplate(master);
			try
			{
				result = temp.query(updateQuery, mapper);
			}
			catch (Exception e)
			{
				throw new PortKeyException("Shard is down");
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
				catch (Exception e)
				{
					logger.info("Exception while reading from slave:" + slave + "\n" + e);
					continue;
				}
				return result;
			}
			temp = new JdbcTemplate(master);
			try
			{
				result = temp.query(updateQuery, mapper);
			}
			catch (Exception e)
			{
				throw new PortKeyException("Shard is down");
			}
			return result;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.Map,
	 * boolean)
	 */
	public List<? extends Entity> getByCriteria(Class<? extends Entity> clazz, Map<String, Object> criteria)
	        throws PortKeyException
	{
		return getByCriteria(clazz, criteria, false);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.List,
	 * java.util.Map)
	 */
	public List<? extends Entity> getByCriteria(Class<? extends Entity> clazz, List<String> attributeNames,
	        Map<String, Object> criteria, boolean readMaster) throws PortKeyException
	{
		RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getMetaData(clazz);
		String getQuery =
		        RdbmsQueryBuilder.getInstance().getGetByCriteriaQuery(tableMetaData, attributeNames, criteria);
		RdbmsMapper<? extends Entity> mapper = RdbmsMapper.getInstance(clazz);
		JdbcTemplate temp;
		List<? extends Entity> result;
		if (readMaster)
		{
			temp = new JdbcTemplate(master);
			try
			{
				result = temp.query(getQuery, mapper);
			}
			catch (Exception e)
			{
				throw new PortKeyException("Shard is down");
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
					logger.info("Exception while reading from slave:" + slave + "\n" + e);
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
				throw new PortKeyException("Shard is down");
			}
			return result;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.List,
	 * java.util.Map, boolean)
	 */
	public List<? extends Entity> getByCriteria(Class<? extends Entity> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws PortKeyException
	{
		return getByCriteria(clazz, attributeNames, criteria, false);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.Class, java.lang.String,
	 * java.util.Map)
	 */
	public List<? extends Entity> getBySql(Class<? extends Entity> clazz, String sql, Map<String, Object> criteria,
	        boolean readMaster) throws PortKeyException
	{
		RdbmsMapper<? extends Entity> mapper = RdbmsMapper.getInstance(clazz);
		JdbcTemplate temp;
		List<? extends Entity> result;
		if (readMaster)
		{
			temp = new JdbcTemplate(master);
			try
			{
				result = temp.query(sql, mapper);
			}
			catch (Exception e)
			{
				throw new PortKeyException("Shard is down");
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
					logger.info("Exception while reading from slave:" + slave + "\n" + e);
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
				throw new PortKeyException("Shard is down");
			}
			return result;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.Class, java.lang.String,
	 * java.util.Map, boolean)
	 */
	public List<? extends Entity> getBySql(Class<? extends Entity> clazz, String sql, Map<String, Object> criteria)
	        throws PortKeyException
	{
		return getBySql(clazz, sql, criteria, false);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.String, java.util.Map)
	 */
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria, boolean readMaster)
	        throws PortKeyException
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
				throw new PortKeyException("Shard is down");
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
					logger.info("Exception while reading from slave:" + slave + "\n" + e);
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
				throw new PortKeyException("Shard is down");
			}
			return result;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.String, java.util.Map,
	 * boolean)
	 */
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria) throws PortKeyException
	{
		return getBySql(sql, criteria, false);
	}
}
