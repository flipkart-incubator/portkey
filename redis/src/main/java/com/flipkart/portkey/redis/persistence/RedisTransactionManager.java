package com.flipkart.portkey.redis.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.exception.QueryNotSupportedException;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.common.persistence.TransactionManager;

public class RedisTransactionManager extends RedisPersistenceManager implements TransactionManager
{
	private List<Entity> operations;
	private Jedis jedis;
	private Transaction t;

	public RedisTransactionManager(Jedis jedis)
	{
		this.jedis = jedis;
		this.t = this.jedis.multi();
	}

	@Override
	public void begin()
	{
		operations = new ArrayList<Entity>();
	}

	@Override
	public void commit() throws ShardNotAvailableException
	{
		for (Entity e : operations)
		{
			insert(e, t);
		}
		t.exec();
		jedis.close();
	}

	@Override
	public void rollback()
	{
		jedis.close();
	}

	@Override
	public <T extends Entity> int insert(T bean) throws QueryExecutionException
	{
		operations.add(bean);
		return 1;
	}

	@Override
	public <T extends Entity> int upsert(T bean) throws QueryExecutionException
	{
		return insert(bean);
	}

	@Override
	public <T extends Entity> int update(T bean) throws QueryExecutionException
	{
		return insert(bean);
	}

	@Override
	public <T extends Entity> int delete(Class<T> clazz, Map<String, Object> criteria) throws QueryExecutionException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}

	@Override
	public <T extends Entity> int upsert(T bean, List<String> columnsToBeUpdatedOnDuplicate)
	        throws QueryExecutionException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}

	@Override
	public <T extends Entity> int update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}

	@Override
	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}

	@Override
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria) throws QueryExecutionException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}

	@Override
	public int updateBySql(String sql, Map<String, Object> criteria) throws QueryExecutionException
	{
		throw new QueryNotSupportedException("Method not supported for redis implementation");
	}
}
