/**
 * 
 */
package com.flipkart.portkey.common.sharding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

/**
 * @author santosh.p
 */
public class ShardLifeCycleManagerImpl implements ShardLifeCycleManager
{
	private static final Logger logger = LoggerFactory.getLogger(ShardLifeCycleManagerImpl.class);
	private Map<DataStoreType, Table<String, String, ShardStatus>> dataStoreTypeToShardStatusTableMap;
	private static ShardLifeCycleManagerImpl instance = null;

	public static ShardLifeCycleManagerImpl getInstance(DataStoreType dataStoreType)
	{
		if (instance == null)
		{
			instance = new ShardLifeCycleManagerImpl();
		}
		instance.addDataStore(dataStoreType);
		return instance;
	}

	private void addDataStore(DataStoreType dataStoreType)
	{
		if (dataStoreTypeToShardStatusTableMap.containsKey(dataStoreType))
		{
			logger.info("Data store type " + dataStoreType + " already exists in map");
			return;
		}
		Table<String, String, ShardStatus> shardStatusTable = HashBasedTable.create();
		dataStoreTypeToShardStatusTableMap.put(dataStoreType, shardStatusTable);
	}

	protected ShardLifeCycleManagerImpl()
	{
		dataStoreTypeToShardStatusTableMap = new HashMap<DataStoreType, Table<String, String, ShardStatus>>();
	}

	@Override
	public void setShardStatus(DataStoreType dataStoreType, String databaseName, String shardId, ShardStatus shardStatus)
	{
		Table<String, String, ShardStatus> shardStatusTable = dataStoreTypeToShardStatusTableMap.get(dataStoreType);
		shardStatusTable.put(databaseName, shardId, shardStatus);
		dataStoreTypeToShardStatusTableMap.put(dataStoreType, shardStatusTable);
	}

	@Override
	public void setShardStatusMap(DataStoreType dataStoreType, String databaseName,
	        Map<String, ShardStatus> shardStatusMap)
	{
		Table<String, String, ShardStatus> shardStatusTable = dataStoreTypeToShardStatusTableMap.get(dataStoreType);
		for (String shardId : shardStatusMap.keySet())
		{
			shardStatusTable.put(databaseName, shardId, shardStatusMap.get(shardId));
		}
		dataStoreTypeToShardStatusTableMap.put(dataStoreType, shardStatusTable);
	}

	@Override
	public List<String> getShardListForStatus(DataStoreType dataStoreType, String databaseName, ShardStatus shardStatus)
	{
		Table<String, String, ShardStatus> shardStatusTable = dataStoreTypeToShardStatusTableMap.get(dataStoreType);
		Map<String, ShardStatus> shardStatusMap = shardStatusTable.row(databaseName);
		List<String> shardList = new ArrayList<String>();
		for (String shardId : shardStatusMap.keySet())
		{
			if (shardStatusMap.get(shardId) == shardStatus)
			{
				shardList.add(shardId);
			}
		}
		return shardList;
	}

	@Override
	public ShardStatus getShardStatus(DataStoreType dataStoreType, String databaseName, String shardId)
	{
		Table<String, String, ShardStatus> shardStatusTable = dataStoreTypeToShardStatusTableMap.get(dataStoreType);
		return shardStatusTable.get(databaseName, shardId);
	}

	@Override
	public Map<String, ShardStatus> getShardStatusMap(DataStoreType dataStoreType, String databaseName)
	{
		Table<String, String, ShardStatus> shardStatusTable = dataStoreTypeToShardStatusTableMap.get(dataStoreType);
		return shardStatusTable.row(databaseName);
	}
}
