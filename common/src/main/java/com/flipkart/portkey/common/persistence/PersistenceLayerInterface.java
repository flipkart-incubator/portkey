/**
 * 
 */
package com.flipkart.portkey.common.persistence;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.persistence.query.PortKeyQuery;

/**
 * @author santosh.p
 */
public interface PersistenceLayerInterface
{

	public <T extends Entity> Result insert(T bean) throws PortKeyException;

	public <T extends Entity> Result insert(T bean, boolean generateShardId) throws PortKeyException;

	public <T extends Entity> Result upsert(T bean) throws QueryExecutionException;

	public <T extends Entity> Result upsert(T bean, List<String> updateFields) throws PortKeyException;

	public <T extends Entity> Result update(T bean) throws PortKeyException;

	public <T extends Entity> Result update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws PortKeyException;

	public <T extends Entity> void executeTransaction(List<PortKeyQuery> queries) throws PortKeyException;

	public <T extends Entity> Result delete(Class<T> clazz, Map<String, Object> criteria) throws PortKeyException;

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws PortKeyException;

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws PortKeyException;

	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws PortKeyException;

	public List<Map<String, Object>> getBySql(String databaseName, String sql, Map<String, Object> criteria)
	        throws PortKeyException;

	public <T extends Entity> List<T> getBySql(Class<T> clazz, Map<DataStoreType, String> sqlMap,
	        Map<String, Object> criteria) throws PortKeyException;

	public List<Map<String, Object>> getBySql(Map<DataStoreType, String> datastoreToDatabaseNameMap,
	        Map<DataStoreType, String> sqlMap, Map<String, Object> criteria) throws PortKeyException;

	public Result updateBySql(String databaseName, String sql, Map<String, Object> criteria) throws PortKeyException;
}
