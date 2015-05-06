package com.flipkart.portkey.persistence;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.persistence.PersistenceLayerInterface;
import com.flipkart.portkey.common.persistence.Result;
import com.flipkart.portkey.common.persistence.TransactionManager;
import com.flipkart.portkey.common.persistence.query.UpdateQuery;

public class TransactionLayer implements PersistenceLayerInterface
{
	private Map<DataStoreType, TransactionManager> dataStoreTypeToTransactionManagerMap =
	        new HashMap<DataStoreType, TransactionManager>();

	public TransactionLayer(Map<DataStoreType, TransactionManager> dataStoreTypeToTransactionManagerMap)
	{
		this.dataStoreTypeToTransactionManagerMap = dataStoreTypeToTransactionManagerMap;
	}

	public TransactionLayer()
	{
	}

	public void begin()
	{

	}

	public void commit(DataStoreType type)
	{

	}

	public void rollback()
	{

	}

	@Override
	public <T extends Entity> Result insert(T bean) throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> Result insert(T bean, boolean generateShardId) throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> Result upsert(T bean) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> Result upsert(T bean, List<String> updateFields) throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> Result update(T bean) throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result update(List<UpdateQuery> queries) throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result update(List<UpdateQuery> queries, boolean failIfNoRowsAreUpdated) throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> Result update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> Result delete(Class<T> clazz, Map<String, Object> criteria) throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Map<String, Object>> getBySql(String databaseName, String sql, Map<String, Object> criteria)
	        throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> List<T> getBySql(Class<T> clazz, Map<DataStoreType, String> sqlMap,
	        Map<String, Object> criteria) throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Map<String, Object>> getBySql(Map<DataStoreType, String> datastoreToDatabaseNameMap,
	        Map<DataStoreType, String> sqlMap, Map<String, Object> criteria) throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result updateBySql(String databaseName, String sql, Map<String, Object> criteria) throws PortKeyException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> void insert(List<T> beans) throws PortKeyException
	{
		// TODO Auto-generated method stub

	}

}
