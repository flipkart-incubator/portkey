package com.flipkart.portkey.redis.persistence;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.InvalidAnnotationException;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.redis.keyparser.DefaultKeyParser;
import com.flipkart.portkey.redis.keyparser.KeyParserInterface;
import com.flipkart.portkey.redis.mapper.DefaultRedisMapper;
import com.flipkart.portkey.redis.mapper.RedisMapper;
import com.flipkart.portkey.redis.metadata.RedisMetaData;
import com.flipkart.portkey.redis.metadata.RedisMetaDataCache;

public class RedisPersistenceManager
{
	KeyParserInterface keyParser = new DefaultKeyParser();
	RedisMapper mapper = new DefaultRedisMapper();

	protected <T extends Entity> RedisMetaData getMetaData(Class<T> clazz)
	{
		return RedisMetaDataCache.getInstance().getMetaData(clazz);
	}

	protected <T extends Entity> int insert(T bean, Jedis jedis) throws ShardNotAvailableException
	{
		RedisMetaData metaData = getMetaData(bean.getClass());
		List<String> keyList = keyParser.parsePrimaryKeyPattern(bean, metaData);
		String primaryKey = null;
		if (keyList.size() == 1)
		{
			String key = keyList.get(0);
			String serialized = null;
			serialized = mapper.serialize(bean);
			jedis.set(key, serialized);
			primaryKey = key;
		}
		else if (keyList.size() == 2)
		{
			String key = keyList.get(0);
			String field = keyList.get(1);
			String serialized = null;
			serialized = mapper.serialize(bean);
			jedis.hset(key, field, serialized);
			primaryKey = key + ":" + field;
		}
		else
		{
			throw new InvalidAnnotationException("Invalid key format, key:" + keyList);
		}
		List<String> secondaryKeys = keyParser.parseSecondaryKeyPatterns(bean, metaData);
		for (String secondaryKey : secondaryKeys)
		{
			jedis.set(secondaryKey, primaryKey);
		}
		return 1;
	}

	protected <T extends Entity> int insert(T bean, Transaction t) throws ShardNotAvailableException
	{
		RedisMetaData metaData = getMetaData(bean.getClass());
		List<String> keyList = keyParser.parsePrimaryKeyPattern(bean, metaData);
		String primaryKey = null;
		if (keyList.size() == 1)
		{
			String key = keyList.get(0);
			String serialized = null;
			serialized = mapper.serialize(bean);
			t.set(key, serialized);
			primaryKey = key;
		}
		else if (keyList.size() == 2)
		{
			String key = keyList.get(0);
			String field = keyList.get(1);
			String serialized = null;
			serialized = mapper.serialize(bean);
			t.hset(key, field, serialized);
			primaryKey = key + ":" + field;
		}
		else
		{
			throw new InvalidAnnotationException("Invalid key format, key:" + keyList);
		}
		List<String> secondaryKeys = keyParser.parseSecondaryKeyPatterns(bean, metaData);
		for (String secondaryKey : secondaryKeys)
		{
			t.set(secondaryKey, primaryKey);
		}
		return 1;
	}
}
