/**
 * 
 */
package com.flipkart.portkey.common.persistence;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.exception.QueryNotSupportedException;
import com.flipkart.portkey.common.exception.ShardNotAvailableException;

/**
 * @author santosh.p
 */
public interface PersistenceManager
{
	public ShardStatus healthCheck();

	public <T extends Entity> int insert(T bean) throws QueryExecutionException;

	public <T extends Entity> int update(T bean) throws QueryExecutionException;

	public <T extends Entity> int update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws PortKeyException;

	public <T extends Entity> int delete(Class<T> clazz, Map<String, Object> criteria)
	        throws QueryNotSupportedException, ShardNotAvailableException;

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws PortKeyException;

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria, boolean readMaster)
	        throws PortKeyException;

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws PortKeyException;

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria, boolean readMaster) throws PortKeyException;

	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws PortKeyException;

	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria,
	        boolean readMaster) throws PortKeyException;

	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria) throws PortKeyException;

	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria, boolean readMaster)
	        throws PortKeyException;

}
