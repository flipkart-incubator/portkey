/**
 * 
 */
package com.flipkart.portkey.common.sharding;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 * @author santosh.p
 */
public class ShardLifeCycleManagerImpl implements ShardLifeCycleManager
{
	private static final Logger logger = Logger.getLogger(ShardLifeCycleManagerImpl.class);
	private Config cfg = new Config();
	private IMap<DataStoreType, Table<String, String, ShardStatus>> dataStoreTypeToShardStatusTableMap;
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
			logger.info("Data store type " + dataStoreType + " already exists in hazelcast map");
			return;
		}
		Table<String, String, ShardStatus> shardStatusTable = HashBasedTable.create();
		dataStoreTypeToShardStatusTableMap.put(dataStoreType, shardStatusTable);
	}

	protected ShardLifeCycleManagerImpl()
	{
		HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(cfg);
		dataStoreTypeToShardStatusTableMap = hazelcastInstance.getMap("liveShards");
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
