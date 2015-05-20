package com.flipkart.portkey.common.persistence;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.exception.QueryExecutionException;

public interface TransactionManager
{

	public void begin();

	public void commit() throws PortKeyException;

	public void rollback();

	public <T extends Entity> int insert(T bean) throws QueryExecutionException;

	public <T extends Entity> int upsert(T bean) throws QueryExecutionException;

	public <T extends Entity> int upsert(T bean, List<String> columnsToBeUpdatedOnDuplicate)
	        throws QueryExecutionException;

	public <T extends Entity> int update(T bean) throws QueryExecutionException;

	public <T extends Entity> int update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws QueryExecutionException;

	public <T extends Entity> int delete(Class<T> clazz, Map<String, Object> criteria) throws QueryExecutionException;

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws QueryExecutionException;

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws QueryExecutionException;

	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException;

	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria) throws QueryExecutionException;

	public int updateBySql(String sql, Map<String, Object> criteria) throws QueryExecutionException;
}
