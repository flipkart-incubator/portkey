package com.flipkart.portkey.redis.persistence;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.persistence.TransactionManager;

public class RedisTransactionManager implements TransactionManager
{

	@Override
	public void begin()
	{
	}

	@Override
	public void commit()
	{
		// TODO Auto-generated method stub
	}

	@Override
	public void rollback()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public <T extends Entity> int insert(T bean) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <T extends Entity> int upsert(T bean) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <T extends Entity> int upsert(T bean, List<String> columnsToBeUpdatedOnDuplicate)
	        throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <T extends Entity> int update(T bean) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <T extends Entity> int update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <T extends Entity> int delete(Class<T> clazz, Map<String, Object> criteria) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int updateBySql(String sql, Map<String, Object> criteria) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return 0;
	}

}
