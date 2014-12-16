/**
 * 
 */
package com.flipkart.portkey.redis.persistence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.PortKeyException;
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
public class RedisPersistenceManager implements PersistenceManager
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

	public RedisPersistenceManager(String host, int port)
	{
		this.host = host;
		this.port = port;
		initializeConnectionPool();
	}

	public RedisPersistenceManager(String host, int port, JedisPoolConfig poolConfig)
	{
		this.host = host;
		this.port = port;
		initializeConnectionPool();
		this.poolConfig = poolConfig;
	}

	public RedisPersistenceManager(String host, int port, int database, String password)
	{
		this.host = host;
		this.port = port;
		this.database = database;
		this.password = password;
		initializeConnectionPool();
	}

	public RedisPersistenceManager(String host, int port, int database, String password, JedisPoolConfig poolConfig)
	{
		this.host = host;
		this.port = port;
		this.database = database;
		this.password = password;
		this.poolConfig = poolConfig;
		initializeConnectionPool();
	}

	/**
	 * 
	 */
	private void initializeConnectionPool()
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
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#healthCheck()
	 */
	public ShardStatus healthCheck()
	{
		Jedis conn = cm.getConnection();
		if (conn.ping() == "PONG")
		{
			return ShardStatus.AVAILABLE_FOR_WRITE;
		}
		else
		{
			return ShardStatus.UNAVAILABLE;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#insert(com.flipkart.portkey.common.entity.Entity)
	 */
	public int insert(Entity bean) throws PortKeyException
	{
		RedisMetaData metaData = RedisMetaDataCache.getMetaData(bean.getClass());
		List<String> keys = keyParser.parsePrimaryKeyPattern(bean, metaData);

		Jedis conn = cm.getConnection();
		if (keys.size() == 1)
		{
			String key = keys.get(0);
			String serialized = null;
			try
			{
				serialized = mapper.serialize(bean);
			}
			catch (JsonGenerationException e)
			{
				throw new PortKeyException("Failed to serialize bean", e);
			}
			catch (JsonMappingException e)
			{
				throw new PortKeyException("Failed to serialize bean", e);
			}
			catch (IOException e)
			{
				throw new PortKeyException("Failed to serialize bean", e);
			}
			conn.set(key, serialized);
			return 1;
		}
		if (keys.size() == 2)
		{
			String outerKey = keys.get(0);
			String innerKey = keys.get(1);
			String serialized = null;
			try
			{
				serialized = mapper.serialize(bean);
			}
			catch (JsonGenerationException e)
			{
				throw new PortKeyException("Failed to serialize bean", e);
			}
			catch (JsonMappingException e)
			{
				throw new PortKeyException("Failed to serialize bean", e);
			}
			catch (IOException e)
			{
				throw new PortKeyException("Failed to serialize bean", e);
			}
			Long retVal = conn.hset(outerKey, innerKey, serialized);
			if (retVal == 0)
			{
				logger.warn("Key already exists in redis: Outer key:" + outerKey + "Inner key:" + innerKey);
			}
			return 1;
		}
		throw new PortKeyException("Key size more than 2 not supported.");
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#update(com.flipkart.portkey.common.entity.Entity)
	 */
	public int update(Entity bean) throws PortKeyException
	{
		// TODO: check if following line is compatible
		return insert(bean);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#update(java.lang.Class, java.util.Map,
	 * java.util.Map)
	 */
	public int update(Class<? extends Entity> clazz, Map<String, Object> updateValuesMap, Map<String, Object> criteria)
	        throws PortKeyException
	{
		throw new PortKeyException("Method not supported for redis implementation");
	}

	private void deleteAllSecondaryKeys(Entity bean) throws PortKeyException
	{
		RedisMetaData metaData = RedisMetaDataCache.getMetaData(bean.getClass());
		List<String> secondaryKeys = keyParser.parseSecondaryKeyPatterns(bean, metaData);
		Jedis conn = cm.getConnection();
		for (String key : secondaryKeys)
		{
			conn.del(key);
		}
	}

	// TODO: make sure the hashset gets deleted when last pojo in it gets deleted
	private void deletePrimaryKey(Entity bean) throws PortKeyException
	{
		RedisMetaData metaData = RedisMetaDataCache.getMetaData(bean.getClass());
		List<String> primaryKeys = keyParser.parsePrimaryKeyPattern(bean, metaData);
		Jedis conn = cm.getConnection();
		if (primaryKeys.size() == 1)
		{
			String key = primaryKeys.get(0);
			conn.del(key);
		}
		else if (primaryKeys.size() == 2)
		{
			String outerKey = primaryKeys.get(0);
			String innerKey = primaryKeys.get(1);
			conn.hdel(outerKey, innerKey);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#delete(java.lang.Class, java.util.Map)
	 */
	public int delete(Class<? extends Entity> clazz, Map<String, Object> criteria) throws PortKeyException
	{
		List<? extends Entity> beans = getByCriteria(clazz, criteria, true);
		Entity bean = beans.get(0);
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
	public List<? extends Entity> getByCriteria(Class<? extends Entity> clazz, Map<String, Object> criteria)
	        throws PortKeyException
	{
		return getByCriteria(clazz, criteria, false);
	}

	private Entity getEntityFromPrimaryKeyAttributes(Class<? extends Entity> clazz, Map<String, Object> criteria,
	        RedisMetaData metaData) throws PortKeyException
	{
		Entity bean;
		List<String> keys;
		String value;
		Jedis conn = cm.getConnection();
		keys = keyParser.parsePrimaryKeyPattern(criteria, metaData);
		if (keys.size() == 1)
		{
			value = conn.get(keys.get(0));
		}
		else if (keys.size() == 2)
		{
			value = conn.hget(keys.get(0), keys.get(1));
		}
		else
		{
			throw new PortKeyException("Key size more than 2 not supported.");
		}
		try
		{
			bean = mapper.deserialize(value, clazz);
		}
		catch (JsonParseException e)
		{
			throw new PortKeyException("Failed to parse json", e);
		}
		catch (JsonMappingException e)
		{
			throw new PortKeyException("Failed to parse json", e);
		}
		catch (IOException e)
		{
			throw new PortKeyException("Failed to parse json", e);
		}
		return bean;
	}

	private Entity getEntityFromSecondaryKeyAttributes(Class<? extends Entity> clazz, Map<String, Object> criteria,
	        RedisMetaData metaData) throws PortKeyException
	{
		Entity bean;
		String key;
		String primaryKey;
		String value;
		Jedis conn = cm.getConnection();
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
			throw new PortKeyException("Key size more than 2 not supported.");
		}
		try
		{
			bean = mapper.deserialize(value, clazz);
		}
		catch (JsonParseException e)
		{
			throw new PortKeyException("Failed to parse json", e);
		}
		catch (JsonMappingException e)
		{
			throw new PortKeyException("Failed to parse json", e);
		}
		catch (IOException e)
		{
			throw new PortKeyException("Failed to parse json", e);
		}
		return bean;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.Map,
	 * boolean)
	 */
	public List<? extends Entity> getByCriteria(Class<? extends Entity> clazz, Map<String, Object> criteria,
	        boolean readMaster) throws PortKeyException
	{
		List<Entity> retVal = new ArrayList<Entity>();
		RedisMetaData metaData = RedisMetaDataCache.getMetaData(clazz);
		Set<String> criteriaAttributes = criteria.keySet();
		Set<String> primaryKeyAttributes = new HashSet<String>(metaData.getPrimaryKeyAttributes());
		Entity bean;
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
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.List,
	 * java.util.Map)
	 */
	public List<? extends Entity> getByCriteria(Class<? extends Entity> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws PortKeyException
	{
		return getByCriteria(clazz, attributeNames, criteria, false);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByCriteria(java.lang.Class, java.util.List,
	 * java.util.Map, boolean)
	 */
	public List<? extends Entity> getByCriteria(Class<? extends Entity> clazz, List<String> attributeNames,
	        Map<String, Object> criteria, boolean readMaster) throws PortKeyException
	{
		return getByCriteria(clazz, criteria, readMaster);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.Class, java.lang.String,
	 * java.util.Map)
	 */
	public List<? extends Entity> getBySql(Class<? extends Entity> clazz, String sql, Map<String, Object> criteria)
	        throws PortKeyException
	{
		throw new PortKeyException("Method not supported for redis implementation");
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.Class, java.lang.String,
	 * java.util.Map, boolean)
	 */
	public List<? extends Entity> getBySql(Class<? extends Entity> clazz, String sql, Map<String, Object> criteria,
	        boolean readMaster) throws PortKeyException
	{
		throw new PortKeyException("Method not supported for redis implementation");
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.String, java.util.Map)
	 */
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria) throws PortKeyException
	{
		throw new PortKeyException("Method not supported for redis implementation");
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.String, java.util.Map,
	 * boolean)
	 */
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria, boolean readMaster)
	        throws PortKeyException
	{
		throw new PortKeyException("Method not supported for redis implementation");
	}
}
