/**
 * 
 */
package com.flipkart.portkey.redis.connection;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author santosh.p
 */
public class ConnectionManager
{
	private String host = "localhost";
	private int port = 6379;

	private int database = 0;
	private String password = null;

	private boolean blockWhenExhausted = true;
	private boolean jmxEnabled = true;
	private String jmxPrefix = "redis";
	private int maxIdle = 10000;
	private int maxTotal = 5000;
	private long maxWaitMillis = 1000;
	private long minEvictableIdleTimeMillis = 3600000;
	private int minIdle = 1000;
	private boolean testOnBorrow = false;
	private boolean testOnReturn = false;
	private long timeBetweenEvictionRunsMillis = 3600000;

	private static Map<String, JedisPool> jedisPoolMap = new HashMap<String, JedisPool>();

	public ConnectionManager()
	{

	}

	public ConnectionManager(String host, int port)
	{
		this.host = host;
		this.port = port;
	}

	public ConnectionManager(String host, int port, JedisPoolConfig poolConfig)
	{
		this.host = host;
		this.port = port;
		setJedisPoolConfig(poolConfig);
	}

	public void setJedisPoolConfig(JedisPoolConfig poolConfig)
	{
		this.blockWhenExhausted = poolConfig.getBlockWhenExhausted();
		this.jmxEnabled = poolConfig.getJmxEnabled();
		this.jmxPrefix = poolConfig.getJmxNamePrefix();
		this.maxIdle = poolConfig.getMaxIdle();
		this.maxTotal = poolConfig.getMaxTotal();
		this.maxWaitMillis = poolConfig.getMaxWaitMillis();
		this.minEvictableIdleTimeMillis = poolConfig.getMinEvictableIdleTimeMillis();
		this.minIdle = poolConfig.getMinIdle();
		this.testOnBorrow = poolConfig.getTestOnBorrow();
		this.testOnReturn = poolConfig.getTestOnReturn();
		this.timeBetweenEvictionRunsMillis = poolConfig.getTimeBetweenEvictionRunsMillis();
	}

	public String getHost()
	{
		return host;
	}

	public void setHost(String host)
	{
		this.host = host;
	}

	public int getPort()
	{
		return port;
	}

	public void setPort(int port)
	{
		this.port = port;
	}

	public int getDatabase()
	{
		return database;
	}

	public void setDatabase(int database)
	{
		this.database = database;
	}

	public long getMaxWaitMillis()
	{
		return maxWaitMillis;
	}

	public void setMaxWaitMillis(long maxWaitMillis)
	{
		this.maxWaitMillis = maxWaitMillis;
	}

	public int getMaxIdle()
	{
		return maxIdle;
	}

	public void setMaxIdle(int maxIdle)
	{
		this.maxIdle = maxIdle;
	}

	public int getMinIdle()
	{
		return minIdle;
	}

	public void setMinIdle(int minIdle)
	{
		this.minIdle = minIdle;
	}

	public int getMaxTotal()
	{
		return maxTotal;
	}

	public void setMaxTotal(int maxTotal)
	{
		this.maxTotal = maxTotal;
	}

	public boolean isBlockWhenExhausted()
	{
		return blockWhenExhausted;
	}

	public void setBlockWhenExhausted(boolean blockWhenExhausted)
	{
		this.blockWhenExhausted = blockWhenExhausted;
	}

	public boolean isJmxEnabled()
	{
		return jmxEnabled;
	}

	public void setJmxEnabled(boolean jmxEnabled)
	{
		this.jmxEnabled = jmxEnabled;
	}

	public String getJmxPrefix()
	{
		return jmxPrefix;
	}

	public void setJmxPrefix(String jmxPrefix)
	{
		this.jmxPrefix = jmxPrefix;
	}

	public String getPassword()
	{
		return password;
	}

	public void setPassword(String password)
	{
		this.password = password;
	}

	public boolean isTestOnBorrow()
	{
		return testOnBorrow;
	}

	public void setTestOnBorrow(boolean testOnBorrow)
	{
		this.testOnBorrow = testOnBorrow;
	}

	public boolean isTestOnReturn()
	{
		return testOnReturn;
	}

	public void setTestOnReturn(boolean testOnReturn)
	{
		this.testOnReturn = testOnReturn;
	}

	public long getMinEvictableIdleTimeMillis()
	{
		return minEvictableIdleTimeMillis;
	}

	public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis)
	{
		this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
	}

	public long getTimeBetweenEvictionRunsMillis()
	{
		return timeBetweenEvictionRunsMillis;
	}

	public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis)
	{
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
	}

	private String getJedisPoolMapKey()
	{
		return host + ":" + port;
	}

	public void build()
	{
		String key = getJedisPoolMapKey();
		if (!jedisPoolMap.containsKey(key))
		{
			JedisPoolConfig poolConfig = new JedisPoolConfig();
			poolConfig.setMaxWaitMillis(this.maxWaitMillis);
			poolConfig.setMinIdle(this.minIdle);
			poolConfig.setMaxIdle(this.maxIdle);
			poolConfig.setTestOnBorrow(this.testOnBorrow);
			poolConfig.setTestOnReturn(this.testOnReturn);
			poolConfig.setMinEvictableIdleTimeMillis(this.minEvictableIdleTimeMillis);
			poolConfig.setTimeBetweenEvictionRunsMillis(this.timeBetweenEvictionRunsMillis);
			poolConfig.setMaxTotal(this.maxTotal);
			poolConfig.setBlockWhenExhausted(this.blockWhenExhausted);
			poolConfig.setJmxEnabled(this.jmxEnabled);
			poolConfig.setJmxNamePrefix(this.jmxPrefix);
			JedisPool pool = new JedisPool(poolConfig, host, port);
			jedisPoolMap.put(key, pool);
		}
	}

	public Jedis getConnection()
	{
		String key = getJedisPoolMapKey();
		JedisPool pool = jedisPoolMap.get(key);
		if (pool != null)
		{
			return pool.getResource();
		}
		return null;
	}

	public void returnConnection(Jedis connection)
	{
		String key = getJedisPoolMapKey();
		JedisPool pool = jedisPoolMap.get(key);
		pool.returnResource(connection);
	}
}
