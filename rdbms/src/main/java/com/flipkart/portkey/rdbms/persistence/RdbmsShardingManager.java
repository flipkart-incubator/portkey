package com.flipkart.portkey.rdbms.persistence;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.persistence.ShardingManager;
import com.flipkart.portkey.common.sharding.ShardIdentifier;
import com.flipkart.portkey.common.sharding.ShardLifeCycleManager;
import com.flipkart.portkey.common.util.PortKeyUtils;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;
import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;

public class RdbmsShardingManager implements ShardingManager
{
	private static Map<String, RdbmsDatabaseConfig> databaseNameToDatabaseConfigMap;
	private static RdbmsMetaDataCache metaDataCache;

	@Override
	public <T extends Entity> int insert(T bean) throws QueryExecutionException
	{
		String databaseName = metaDataCache.getMetaData(bean.getClass()).getDatabaseName();
		RdbmsDatabaseConfig databaseConfig = databaseNameToDatabaseConfigMap.get(databaseName);
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(bean.getClass());
		String shardKeyFieldName = metaData.getShardKeyFieldName();
		String shardKey = PortKeyUtils.toString(PortKeyUtils.getFieldValueFromBean(bean, shardKeyFieldName));
		ShardIdentifier shardIdentifier = databaseConfig.getShardIdentifier();
		List<String> liveShards =
		        ShardLifeCycleManager.getInstance().getShardListForStatus(DataStoreType.RDBMS,
		                ShardStatus.AVAILABLE_FOR_WRITE);
		String shardId = shardIdentifier.getShardId(shardKey, liveShards);
		RdbmsPersistenceManager pm = databaseConfig.getPersistenceManager(shardId);
		return pm.insert(bean);
	}

	@Override
	public <T extends Entity> int upsert(T bean) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <T extends Entity> int upsert(T bean, List<String> columnsToBeUpdatedOnDuplicate)
	        throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <T extends Entity> int update(T bean) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <T extends Entity> int update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <T extends Entity> int delete(Class<T> clazz, Map<String, Object> criteria) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria, boolean readMaster)
	        throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria, boolean readMaster) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria,
	        boolean readMaster) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria, boolean readMaster)
	        throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int updateBySql(String sql, Map<String, Object> criteria) throws QueryExecutionException
	{
		// TODO Auto-generated method stub
		return 0;
	}

}
