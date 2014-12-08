/**
 * 
 */
package com.flipkart.portkey.persistencelayer;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.persistence.PersistenceLayerInterface;
import com.flipkart.portkey.common.persistence.config.PersistenceConfig;

/**
 * @author santosh.p
 */
public class PersistenceLayer implements PersistenceLayerInterface
{
	private PersistenceConfig persistenceConfig;

	public PersistenceLayer(PersistenceConfig persistenceConfig)
	{
		this.persistenceConfig = persistenceConfig;
	}

	public int insert(Entity bean)
	{
		// TODO Auto-generated method stub
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getBySql(java.lang.String, java.util.Map,
	 * boolean)
	 */
	public List<Entity> getBySql(String sql, Map<String, Object> whereClauseMap, boolean readMaster)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getBySql(java.lang.String, java.util.Map)
	 */
	public List<Entity> getBySql(String sql, Map<String, Object> whereClauseMap)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getByWhereClauseMap(java.lang.Class,
	 * java.util.Map, boolean)
	 */
	public List<Entity> getByWhereClauseMap(Class<? extends Entity> clazz, Map<String, Object> whereClauseMap,
	        boolean readMaster)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getByWhereClauseMap(java.util.Map)
	 */
	public List<Entity> getByWhereClauseMap(Map<String, Object> whereClauseMap)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getByPrimaryKey(com.flipkart.portkey.common
	 * .entity.Entity, boolean)
	 */
	public List<Entity> getByPrimaryKey(Entity bean, boolean readMaster)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#getByPrimaryKey(com.flipkart.portkey.common
	 * .entity.Entity)
	 */
	public List<Entity> getByPrimaryKey(Entity bean)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#updateByPrimaryKey(com.flipkart.portkey.common
	 * .entity.Entity)
	 */
	public int updateByPrimaryKey(Entity bean)
	{
		// TODO Auto-generated method stub
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#updateBySql(java.lang.String,
	 * java.util.Map)
	 */
	public int updateBySql(String sql, Map<String, Object> mapValues)
	{
		// TODO Auto-generated method stub
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceLayerInterface#deleteByWhereClauseMap(java.util.Map)
	 */
	public int deleteByWhereClauseMap(Map<String, Object> whereClauseMap)
	{
		// TODO Auto-generated method stub
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceLayerInterface#deleteByPrimaryKey(com.flipkart.portkey.common
	 * .entity.Entity)
	 */
	public int deleteByPrimaryKey(Entity bean)
	{
		// TODO Auto-generated method stub
		return 0;
	}

}
