/**
 * 
 */
package com.flipkart.portkey.common.persistence;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.DataStore;

/**
 * @author santosh.p
 */
public interface PersistenceLayerInterface
{

	public Result insert(Entity bean);

	public Result insert(Entity bean, boolean generateShardId);

	public Result update(Entity bean);

	public Result update(Class<? extends Entity> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria);

	public Result delete(Class<? extends Entity> clazz, Map<String, Object> criteria);

	public List<Entity> getByCriteria(Class<? extends Entity> clazz, Map<String, Object> criteria);

	// TODO: fix the order of parameters
	public List<Entity> getByCriteria(Map<DataStore, Map<String, Object>> dataStoreToCriteriaMap,
	        Class<? extends Entity> clazz);

	public List<Entity> getByCriteria(Class<? extends Entity> clazz, List<String> attributeNames,
	        Map<String, Object> criteria);

	public List<Entity> getBySql(Class<? extends Entity> clazz, String sql, Map<String, Object> criteria);

	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria);

	public List<Entity> getBySql(Class<? extends Entity> clazz, Map<DataStore, String> sqlMap,
	        Map<String, Object> criteria);

	public List<Map<String, Object>> getBySql(Map<DataStore, String> sqlMap, Map<String, Object> criteria);

}
