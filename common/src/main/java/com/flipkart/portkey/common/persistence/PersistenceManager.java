/**
 * 
 */
package com.flipkart.portkey.common.persistence;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.PortKeyException;

/**
 * @author santosh.p
 */
public interface PersistenceManager
{
	public ShardStatus healthCheck();

	public int insert(Entity bean) throws PortKeyException;

	public int update(Entity bean) throws PortKeyException;

	public int update(Class<? extends Entity> clazz, Map<String, Object> updateValuesMap, Map<String, Object> criteria)
	        throws PortKeyException;

	public int delete(Class<? extends Entity> clazz, Map<String, Object> criteria) throws PortKeyException;

	public List<? extends Entity> getByCriteria(Class<? extends Entity> clazz, Map<String, Object> criteria)
	        throws PortKeyException;

	public List<? extends Entity> getByCriteria(Class<? extends Entity> clazz, Map<String, Object> criteria,
	        boolean readMaster) throws PortKeyException;

	public List<? extends Entity> getByCriteria(Class<? extends Entity> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws PortKeyException;

	public List<? extends Entity> getByCriteria(Class<? extends Entity> clazz, List<String> attributeNames,
	        Map<String, Object> criteria, boolean readMaster) throws PortKeyException;

	public List<? extends Entity> getBySql(Class<? extends Entity> clazz, String sql, Map<String, Object> criteria)
	        throws PortKeyException;

	public List<? extends Entity> getBySql(Class<? extends Entity> clazz, String sql, Map<String, Object> criteria,
	        boolean readMaster) throws PortKeyException;

	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria) throws PortKeyException;

	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria, boolean readMaster)
	        throws PortKeyException;

}
