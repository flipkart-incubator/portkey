package com.flipkart.portkey.persistence;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.persistence.Result;
import com.flipkart.portkey.common.persistence.TransactionManager;

public class TransactionLayer
{
	LinkedHashMap<DataStoreType, TransactionManager> dataStoreTypeToTransactionManagerMap;

	public TransactionLayer(LinkedHashMap<DataStoreType, TransactionManager> dataStoreTypeToTransactionManagerMap)
	{
		this.dataStoreTypeToTransactionManagerMap = dataStoreTypeToTransactionManagerMap;
	}

	public void begin()
	{
		for (TransactionManager t : dataStoreTypeToTransactionManagerMap.values())
		{
			t.begin();
		}
	}

	public void commit() throws PortKeyException
	{
		for (TransactionManager t : dataStoreTypeToTransactionManagerMap.values())
		{
			t.commit();
		}
	}

	public void commit(DataStoreType type) throws PortKeyException
	{
		dataStoreTypeToTransactionManagerMap.get(type).commit();
	}

	public void rollback()
	{
		for (TransactionManager t : dataStoreTypeToTransactionManagerMap.values())
		{
			t.rollback();
		}
	}

	public void rollback(DataStoreType type)
	{
		dataStoreTypeToTransactionManagerMap.get(type).rollback();
	}

	public <T extends Entity> Result insert(T bean) throws QueryExecutionException
	{
		Result result = new Result();
		for (Entry<DataStoreType, TransactionManager> entry : dataStoreTypeToTransactionManagerMap.entrySet())
		{
			result.setRowsUpdatedForDataStore(entry.getKey(), entry.getValue().insert(bean));
		}
		result.setEntity(bean);
		return result;
	}

	public <T extends Entity> Result upsert(T bean) throws QueryExecutionException
	{
		Result result = new Result();
		for (Entry<DataStoreType, TransactionManager> entry : dataStoreTypeToTransactionManagerMap.entrySet())
		{
			result.setRowsUpdatedForDataStore(entry.getKey(), entry.getValue().upsert(bean));
		}
		result.setEntity(bean);
		return result;
	}

	public <T extends Entity> Result upsert(T bean, List<String> columnsToBeUpdatedOnDuplicate)
	        throws QueryExecutionException
	{
		Result result = new Result();
		for (Entry<DataStoreType, TransactionManager> entry : dataStoreTypeToTransactionManagerMap.entrySet())
		{
			result.setRowsUpdatedForDataStore(entry.getKey(),
			        entry.getValue().upsert(bean, columnsToBeUpdatedOnDuplicate));
		}
		result.setEntity(bean);
		return result;
	}

	public <T extends Entity> Result update(T bean) throws QueryExecutionException
	{
		Result result = new Result();
		for (Entry<DataStoreType, TransactionManager> entry : dataStoreTypeToTransactionManagerMap.entrySet())
		{
			result.setRowsUpdatedForDataStore(entry.getKey(), entry.getValue().update(bean));
		}
		result.setEntity(bean);
		return result;
	}

	public <T extends Entity> Result update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		Result result = new Result();
		for (Entry<DataStoreType, TransactionManager> entry : dataStoreTypeToTransactionManagerMap.entrySet())
		{
			result.setRowsUpdatedForDataStore(entry.getKey(), entry.getValue().update(clazz, updateValuesMap, criteria));
		}
		return result;
	}

	public <T extends Entity> Result delete(Class<T> clazz, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		Result result = new Result();
		for (Entry<DataStoreType, TransactionManager> entry : dataStoreTypeToTransactionManagerMap.entrySet())
		{
			result.setRowsUpdatedForDataStore(entry.getKey(), entry.getValue().delete(clazz, criteria));
		}
		return result;
	}
}
