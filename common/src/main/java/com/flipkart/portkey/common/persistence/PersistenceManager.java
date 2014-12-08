/**
 * 
 */
package com.flipkart.portkey.common.persistence;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.ShardStatus;

/**
 * @author santosh.p
 */
public interface PersistenceManager
{
	public ShardStatus healthCheck();

	public int insert(Entity bean);

	public List<Entity> getBySql(String sql, Map<String, Object> whereClauseMap, boolean readMaster);

	public List<Entity> getBySql(String sql, Map<String, Object> whereClauseMap);

	public List<Entity> getByWhereClauseMap(Class<? extends Entity> clazz, Map<String, Object> whereClauseMap,
	        boolean readMaster);

	public List<Entity> getByWhereClauseMap(Map<String, Object> whereClauseMap);

	public List<Entity> getByPrimaryKey(Entity bean, boolean readMaster);

	public List<Entity> getByPrimaryKey(Entity bean);

	public int updateByPrimaryKey(Entity bean);

	public int updateBySql(String sql, Map<String, Object> mapValues);

	public int deleteByWhereClauseMap(Map<String, Object> whereClauseMap);

	public int deleteByPrimaryKey(Entity bean);

}
