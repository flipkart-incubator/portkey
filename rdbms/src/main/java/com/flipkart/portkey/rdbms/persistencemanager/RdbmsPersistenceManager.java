/**
 * 
 */
package com.flipkart.portkey.rdbms.persistencemanager;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.persistence.PersistenceManager;
import com.flipkart.portkey.rdbms.persistencemanager.config.RdbmsPersistenceManagerConfig;

/**
 * @author santosh.p
 */
public class RdbmsPersistenceManager implements PersistenceManager
{
	DataSource master;
	List<DataSource> slaves;

	public RdbmsPersistenceManager(RdbmsPersistenceManagerConfig config)
	{
		this.master = config.getMaster();
		this.slaves = config.getSlaves();
	}

	public ShardStatus healthCheck()
	{
		if (isAvailableForWrite())
			return ShardStatus.AVAILABLE_FOR_WRITE;
		else if (isAvailableForRead())
			return ShardStatus.AVAILABLE_FOR_READ;
		else
			return ShardStatus.UNAVAILABLE;
	}

	/**
	 * @return
	 */
	private boolean isAvailableForWrite()
	{
		try
		{
			new JdbcTemplate(master).execute("SELECT 1 FROM DUAL");
		}
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	/**
	 * @return
	 */
	private boolean isAvailableForRead()
	{
		for (DataSource slave : slaves)
		{
			try
			{
				new JdbcTemplate(slave).execute("SELECT 1 FROM DUAL");
			}
			catch (Exception e)
			{
				continue;
			}
			return true;
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#insert(com.flipkart.portkey.common.entity.Entity)
	 */
	public int insert(Entity bean)
	{
		// TODO Auto-generated method stub
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.String, java.util.Map,
	 * boolean)
	 */
	public List<Entity> getBySql(String sql, Map<String, Object> whereClauseMap, boolean readMaster)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getBySql(java.lang.String, java.util.Map)
	 */
	public List<Entity> getBySql(String sql, Map<String, Object> whereClauseMap)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByWhereClauseMap(java.lang.Class,
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
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#getByWhereClauseMap(java.util.Map)
	 */
	public List<Entity> getByWhereClauseMap(Map<String, Object> whereClauseMap)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceManager#getByPrimaryKey(com.flipkart.portkey.common.entity
	 * .Entity, boolean)
	 */
	public List<Entity> getByPrimaryKey(Entity bean, boolean readMaster)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceManager#getByPrimaryKey(com.flipkart.portkey.common.entity
	 * .Entity)
	 */
	public List<Entity> getByPrimaryKey(Entity bean)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceManager#updateByPrimaryKey(com.flipkart.portkey.common.entity
	 * .Entity)
	 */
	public int updateByPrimaryKey(Entity bean)
	{
		// TODO Auto-generated method stub
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#updateBySql(java.lang.String, java.util.Map)
	 */
	public int updateBySql(String sql, Map<String, Object> mapValues)
	{
		// TODO Auto-generated method stub
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.persistence.PersistenceManager#deleteByWhereClauseMap(java.util.Map)
	 */
	public int deleteByWhereClauseMap(Map<String, Object> whereClauseMap)
	{
		// TODO Auto-generated method stub
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.persistence.PersistenceManager#deleteByPrimaryKey(com.flipkart.portkey.common.entity
	 * .Entity)
	 */
	public int deleteByPrimaryKey(Entity bean)
	{
		// TODO Auto-generated method stub
		return 0;
	}

}
