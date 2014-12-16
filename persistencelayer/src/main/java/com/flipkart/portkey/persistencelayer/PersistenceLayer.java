/**
 * 
 */
package com.flipkart.portkey.persistencelayer;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.DataStore;
import com.flipkart.portkey.common.persistence.PersistenceLayerInterface;
import com.flipkart.portkey.common.persistence.Result;

/**
 * @author santosh.p
 */
public class PersistenceLayer implements PersistenceLayerInterface
{

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#insert(com.flipkart.portkey.common.entity.Entity
	 * )
	 */
	public Result insert(Entity bean)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#insert(com.flipkart.portkey.common.entity.Entity
	 * , boolean)
	 */
	public Result insert(Entity bean, boolean generateShardId)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#update(com.flipkart.portkey.common.entity.Entity
	 * )
	 */
	public Result update(Entity bean)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#update(java.lang.Class, java.util.Map,
	 * java.util.Map)
	 */
	public Result update(Class<? extends Entity> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#delete(java.lang.Class, java.util.Map)
	 */
	public Result delete(Class<? extends Entity> clazz, Map<String, Object> criteria)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getByCriteria(java.lang.Class,
	 * java.util.Map)
	 */
	public List<Entity> getByCriteria(Class<? extends Entity> clazz, Map<String, Object> criteria)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getByCriteria(java.util.Map,
	 * java.lang.Class)
	 */
	public List<Entity> getByCriteria(Map<DataStore, Map<String, Object>> dataStoreToCriteriaMap,
	        Class<? extends Entity> clazz)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getByCriteria(java.lang.Class,
	 * java.util.List, java.util.Map)
	 */
	public List<Entity> getByCriteria(Class<? extends Entity> clazz, List<String> attributeNames,
	        Map<String, Object> criteria)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getBySql(java.lang.Class,
	 * java.lang.String, java.util.Map)
	 */
	public List<Entity> getBySql(Class<? extends Entity> clazz, String sql, Map<String, Object> criteria)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getBySql(java.lang.String, java.util.Map)
	 */
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getBySql(java.lang.Class, java.util.Map,
	 * java.util.Map)
	 */
	public List<Entity> getBySql(Class<? extends Entity> clazz, Map<DataStore, String> sqlMap,
	        Map<String, Object> criteria)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getBySql(java.util.Map, java.util.Map)
	 */
	public List<Map<String, Object>> getBySql(Map<DataStore, String> sqlMap, Map<String, Object> criteria)
	{
		// TODO Auto-generated method stub
		return null;
	}

}
