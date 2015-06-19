package com.flipkart.portkey.common.persistence;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.entity.JoinEntity;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;
import com.flipkart.portkey.common.persistence.query.UpdateQuery;

public interface ShardingManager
{
	public <T extends Entity> T generateShardIdAndUpdateBean(T bean) throws ShardNotAvailableException;

	public void healthCheck();

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

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria, boolean readMaster)
	        throws QueryExecutionException;

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws QueryExecutionException;

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria, boolean readMaster) throws QueryExecutionException;

	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException;

	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria,
	        boolean readMaster) throws QueryExecutionException;

	public List<Map<String, Object>> getBySql(String databaseName, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException;

	public List<Map<String, Object>> getBySql(String databaseName, String sql, Map<String, Object> criteria,
	        boolean readMaster) throws QueryExecutionException;

	public int updateBySql(String databaseName, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException;

	public int update(List<UpdateQuery> queries) throws QueryExecutionException;

	public int update(List<UpdateQuery> queries, boolean failIfNoRowsAreUpdated) throws QueryExecutionException;

	public <T extends Entity> int insert(List<T> beans) throws QueryExecutionException;

	public <T extends Entity> TransactionManager getTransactionManager(T bean) throws PortKeyException;

	public <T extends JoinEntity> List<T> getByJoinCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws QueryExecutionException;

	public <T extends JoinEntity> List<T> getByJoinCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria, boolean readMaster) throws QueryExecutionException;
}
