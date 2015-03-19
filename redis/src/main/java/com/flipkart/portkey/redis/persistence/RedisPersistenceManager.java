/**
 * 
 */
package com.flipkart.portkey.redis.persistence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.InvalidAnnotationException;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.exception.QueryNotSupportedException;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.common.persistence.PersistenceManager;
import com.flipkart.portkey.redis.connection.ConnectionManager;
import com.flipkart.portkey.redis.keyparser.DefaultKeyParser;
import com.flipkart.portkey.redis.keyparser.KeyParserInterface;
import com.flipkart.portkey.redis.mapper.DefaultRedisMapper;
import com.flipkart.portkey.redis.mapper.RedisMapper;
import com.flipkart.portkey.redis.metadata.RedisMetaData;
import com.flipkart.portkey.redis.metadata.RedisMetaDataCache;

/**
 * @author santosh.p
 */
public class RedisPersistenceManager implements PersistenceManager, InitializingBean
{
	private static final Logger logger = Logger.getLogger(RedisPersistenceManager.class);
	String host = "localhost";
	int port = 6379;
	int database = 0;
	String password;
	JedisPoolConfig poolConfig = null;

	ConnectionManager cm;
	KeyParserInterface keyParser = new DefaultKeyParser();
	RedisMapper mapper = new DefaultRedisMapper();

	public void setHost(String host)
	{
		this.host = host;
	}

	public void setPort(int port)
	{
		this.port = port;
	}

	public void setDatabase(int database)
	{
		this.database = database;
	}

	public void setPassword(String password)
	{
		this.password = password;
	}

	public void setPoolConfig(JedisPoolConfig poolConfig)
	{
		this.poolConfig = poolConfig;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception
	{
		cm = new ConnectionManager();
		cm.setHost(this.host);
		cm.setPort(this.port);
		cm.setDatabase(this.database);
		cm.setPassword(this.password);
		if (poolConfig != null)
		{
			cm.setJedisPoolConfig(poolConfig);
		}
		cm.build();
		logger.info("initialized connection manager");
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#healthCheck()
	 */
	@Override
	public ShardStatus healthCheck()
	{
		logger.debug("health checking for redis, host=" + host + " port=" + port);
		Jedis conn = null;
		try
		{
			conn = cm.getConnection();
			if (conn == null)
			{
				return ShardStatus.UNAVAILABLE;
			}
			logger.debug("acquired redis connection, checking for response");
			if (conn.ping().equals("PONG"))
			{
				logger.debug("instance is available");
				return ShardStatus.AVAILABLE_FOR_WRITE;
			}
			else
			{
				logger.debug("instance is unavailable");
				return ShardStatus.UNAVAILABLE;
			}
		}
		catch (JedisConnectionException e)
		{
			return ShardStatus.UNAVAILABLE;
		}
		finally
		{
			cm.returnConnection(conn);
		}
	}

	private <T extends Entity> RedisMetaData getMetaData(Class<T> clazz)
	{
		return RedisMetaDataCache.getInstance().getMetaData(clazz);
	}

	@Override
	public <T extends Entity> int insert(T bean) throws ShardNotAvailableException
	{
		RedisMetaData metaData = getMetaData(bean.getClass());
		List<String> keys = keyParser.parsePrimaryKeyPattern(bean, metaData);
		String primaryKey = null;
		Jedis conn = null;
		try
		{
			conn = cm.getConnection();
			if (conn == null)
			{
				throw new ShardNotAvailableException("Failed to acquire redis connection, bean=" + bean);
			}
			if (keys.size() == 1)
			{
				String key = keys.get(0);
				String serialized = null;
				serialized = mapper.serialize(bean);
				conn.set(key, serialized);
				primaryKey = key;
			}
			else if (keys.size() == 2)
			{
				String key = keys.get(0);
				String field = keys.get(1);
				String serialized = null;
				serialized = mapper.serialize(bean);
				Long retVal = conn.hset(key, field, serialized);
				primaryKey = key + ":" + field;
				if (retVal == 0)
				{
					logger.debug("Key already exists in redis: Outer key:" + key + "Inner key:" + field);
				}
			}
			else
			{
				throw new InvalidAnnotationException("Exception while trying to parse keys:" + keys
				        + "\nKey consists of more than 2 parts.");
			}
		}
		catch (JedisConnectionException e)
		{
			throw new ShardNotAvailableException("Failed to acquire redis connection while trying to insert bean:"
			        + bean);
		}
		finally
		{
			cm.returnConnection(conn);
		}

		List<String> secondaryKeys = keyParser.parseSecondaryKeyPatterns(bean, metaData);
		for (String secondaryKey : secondaryKeys)
		{
			conn.set(secondaryKey, primaryKey);
		}
		return 1;
	}

	@Override
	public <T extends Entity> int upsert(T bean, List<String> updateFields) throws QueryNotSupportedException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#update(com.flipkart.portkey.common.entity.Entity)
	 */
	@Override
	public <T extends Entity> int update(T bean) throws ShardNotAvailableException
	{
		// TODO: check if this has same impact as update
		return insert(bean);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#update(java.lang.Class, java.util.Map,
	 * java.util.Map)
	 */
	@Override
	public <T extends Entity> int update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws QueryNotSupportedException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}

	private <T extends Entity> void deleteAllSecondaryKeys(T bean) throws ShardNotAvailableException
	{
		RedisMetaData metaData = getMetaData(bean.getClass());
		List<String> secondaryKeys = keyParser.parseSecondaryKeyPatterns(bean, metaData);
		Jedis conn = null;
		try
		{
			conn = cm.getConnection();
			for (String key : secondaryKeys)
			{
				conn.del(key);
			}
		}
		catch (JedisConnectionException e)
		{
			throw new ShardNotAvailableException(
			        "Failed to acquire redis connection while trying delete secondary keys for bean:" + bean);
		}
		finally
		{
			cm.returnConnection(conn);
		}
	}

	// TODO: make sure the hashset gets deleted when last pojo in it gets deleted
	private <T extends Entity> void deletePrimaryKey(T bean) throws ShardNotAvailableException
	{
		RedisMetaData metaData = getMetaData(bean.getClass());
		List<String> primaryKeys = keyParser.parsePrimaryKeyPattern(bean, metaData);
		Jedis conn = null;
		try
		{
			conn = cm.getConnection();
			if (primaryKeys.size() == 1)
			{
				String key = primaryKeys.get(0);
				conn.del(key);
			}
			else if (primaryKeys.size() == 2)
			{
				String key = primaryKeys.get(0);
				String field = primaryKeys.get(1);
				conn.hdel(key, field);
			}
		}
		catch (JedisConnectionException e)
		{
			throw new ShardNotAvailableException(
			        "Failed to acquire redis connection while trying to delete primary key of bean:" + bean);
		}
		finally
		{
			cm.returnConnection(conn);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#delete(java.lang.Class, java.util.Map)
	 */
	@Override
	public <T extends Entity> int delete(Class<T> clazz, Map<String, Object> criteria)
	        throws QueryNotSupportedException, ShardNotAvailableException
	{
		// TODO: check if secondary key exists, if not just delete primary key
		List<T> beans = getByCriteria(clazz, criteria, true);
		T bean = beans.get(0);
		if (bean == null)
		{
			return 1;
		}
		deleteAllSecondaryKeys(bean);
		deletePrimaryKey(bean);
		return 1;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.Map)
	 */
	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws QueryNotSupportedException, ShardNotAvailableException
	{
		return getByCriteria(clazz, criteria, false);
	}

	private <T extends Entity> T getEntityFromPrimaryKeyAttributes(Class<T> clazz, Map<String, Object> criteria,
	        RedisMetaData metaData) throws ShardNotAvailableException
	{
		T bean;
		List<String> keys = null;
		String value;
		Jedis conn = null;
		try
		{
			conn = cm.getConnection();
			if (conn == null)
			{
				throw new ShardNotAvailableException("Failed to acquire redis connection");
			}
			keys = keyParser.parsePrimaryKeyPattern(criteria, metaData);
			if (keys.size() == 1)
			{
				String key = keys.get(0);
				value = conn.get(key);
			}
			else if (keys.size() == 2)
			{
				String key = keys.get(0);
				String field = keys.get(1);
				value = conn.hget(key, field);
			}
			else
			{
				throw new InvalidAnnotationException("Exception while trying to parse keys:" + keys
				        + "\nKey size more than 2 is not supported.");
			}
		}
		catch (JedisConnectionException e)
		{
			throw new ShardNotAvailableException(
			        "Failed to acquire redis connection while trying to read bean from key" + keys);
		}
		finally
		{
			cm.returnConnection(conn);
		}
		bean = mapper.deserialize(value, clazz);
		return bean;
	}

	private <T extends Entity> T getEntityFromSecondaryKeyAttributes(Class<T> clazz, Map<String, Object> criteria,
	        RedisMetaData metaData) throws ShardNotAvailableException
	{
		T bean;
		String key = null;
		String primaryKey;
		String value;
		Jedis conn = null;
		try
		{
			conn = cm.getConnection();
			if (conn == null)
			{
				throw new ShardNotAvailableException("Redis is down");
			}
			key = keyParser.parseSecondaryKeyPattern(criteria, metaData);

			primaryKey = conn.get(key);
			List<String> primaryKeys = Arrays.asList(primaryKey.split("->"));
			if (primaryKeys.size() == 1)
			{
				value = conn.get(primaryKeys.get(0));
			}
			else if (primaryKeys.size() == 2)
			{
				value = conn.hget(primaryKeys.get(0), primaryKeys.get(1));
			}
			else
			{
				throw new InvalidAnnotationException("Exception while trying to parse keys:" + primaryKeys
				        + "\nKey size more than 2 is not supported.");
			}
		}
		catch (JedisConnectionException e)
		{
			throw new ShardNotAvailableException(
			        "Failed to acquire redis connection while trying to read bean from key" + key);
		}
		finally
		{
			cm.returnConnection(conn);
		}
		bean = mapper.deserialize(value, clazz);
		return bean;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.Map,
	 * boolean)
	 */
	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria, boolean readMaster)
	        throws QueryNotSupportedException, ShardNotAvailableException
	{
		List<T> retVal = new ArrayList<T>();
		RedisMetaData metaData = getMetaData(clazz);
		Set<String> criteriaAttributes = criteria.keySet();
		Set<String> primaryKeyAttributes = new HashSet<String>(metaData.getPrimaryKeyAttributes());
		T bean;
		if (criteriaAttributes.equals(primaryKeyAttributes))
		{
			bean = getEntityFromPrimaryKeyAttributes(clazz, criteria, metaData);
			retVal.add(bean);
			return retVal;
		}
		List<List<String>> secondaryKeyAttributesList = metaData.getSecondaryKeyAttributesList();
		for (List<String> secondaryKeyAttributes : secondaryKeyAttributesList)
		{
			Set<String> keyAttributes = new HashSet<String>(secondaryKeyAttributes);
			if (criteriaAttributes.equals(keyAttributes))
			{
				bean = getEntityFromSecondaryKeyAttributes(clazz, criteria, metaData);
				retVal.add(bean);
				return retVal;
			}
		}
		throw new QueryNotSupportedException(
		        "Passed criteria contains attributes other than primary or secondary keys.");
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.List,
	 * java.util.Map)
	 */
	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws QueryNotSupportedException, ShardNotAvailableException
	{
		return getByCriteria(clazz, attributeNames, criteria, false);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.List,
	 * java.util.Map, boolean)
	 */
	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria, boolean readMaster) throws QueryNotSupportedException,
	        ShardNotAvailableException
	{
		return getByCriteria(clazz, criteria, readMaster);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.Class, java.lang.String,
	 * java.util.Map)
	 */
	@Override
	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws QueryNotSupportedException

	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.Class, java.lang.String,
	 * java.util.Map, boolean)
	 */
	@Override
	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria,
	        boolean readMaster) throws QueryNotSupportedException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.String, java.util.Map)
	 */
	@Override
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria)
	        throws QueryNotSupportedException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.String, java.util.Map,
	 * boolean)
	 */
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria, boolean readMaster)
	        throws QueryNotSupportedException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}

	@Override
	// TODO:SANTOSH: implement this
	public <T extends Entity> int upsert(T bean) throws QueryExecutionException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}
}
