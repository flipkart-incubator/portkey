/**
 * 
 */
package com.flipkart.portkey.common.sharding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 * @author santosh.p
 */
public class ShardLifeCycleManager implements ShardLifeCycleManagerInterface
{
	private static final Logger logger = Logger.getLogger(ShardLifeCycleManager.class);
	private Config cfg = new Config();
	private List<DataStoreType> dataStoreTypesList;
	private IMap<DataStoreType, Map<String, ShardStatus>> dataStoreTypeToShardStatusMap;
	private static ShardLifeCycleManager instance = null;

	// returns instance of shard life cycle manager
	public static ShardLifeCycleManager getInstance()
	{
		return instance;
	}

	// (re)initializes shard life cycle manager with passed config and returns the (re)initialized instance
	public static ShardLifeCycleManager getInstance(List<DataStoreType> dataStoreTypes)
	{
		instance = new ShardLifeCycleManager(dataStoreTypes);
		return instance;
	}

	private void initializeShardLifeCycleManager()
	{
		logger.info("Initializing shard life cycle manager");
		HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
		dataStoreTypeToShardStatusMap = instance.getMap("liveShards");
		for (DataStoreType dataStoreType : dataStoreTypesList)
		{
			Map<String, ShardStatus> innerMap = new HashMap<String, ShardStatus>();
			dataStoreTypeToShardStatusMap.put(dataStoreType, innerMap);
		}
		logger.info("Initialization complete");
	}

	protected ShardLifeCycleManager(List<DataStoreType> dataStoreTypes)
	{
		this.dataStoreTypesList = dataStoreTypes;
		initializeShardLifeCycleManager();
	}

	public void setShardStatus(DataStoreType dataStoreType, String shardId, ShardStatus shardStatus)
	{
		logger.debug("setting shard status datastoretype=" + dataStoreType + " shardId=" + shardId + " shardStatus="
		        + shardStatus);
		// TODO: handle NullPointerException
		Map<String, ShardStatus> shardToStatusMap = dataStoreTypeToShardStatusMap.get(dataStoreType);
		shardToStatusMap.put(shardId, shardStatus);
		dataStoreTypeToShardStatusMap.put(dataStoreType, shardToStatusMap);
		logger.debug("value set=" + dataStoreTypeToShardStatusMap.get(dataStoreType).get(shardId));
	}

	public List<String> getShardListForStatus(DataStoreType dataStoreType, ShardStatus shardStatus)
	{
		logger.debug("Generating shard list with status=" + shardStatus + " for datastoretype=" + dataStoreType);
		Map<String, ShardStatus> shardToStatusMap = dataStoreTypeToShardStatusMap.get(dataStoreType);
		if (shardToStatusMap == null)
		{
			logger.debug("no shards are registered for datastore type=" + dataStoreType);
			return null;
		}
		List<String> shardList = new ArrayList<String>();
		for (String shardId : shardToStatusMap.keySet())
		{
			if (shardToStatusMap.get(shardId) == shardStatus)
			{
				shardList.add(shardId);
			}
		}
		logger.debug("shard list=" + shardList);
		return shardList.size() > 0 ? shardList : null;
	}

	public ShardStatus getShardStatus(DataStoreType dataStoreType, String shardId)
	{
		Map<String, ShardStatus> shardToStatusMap = dataStoreTypeToShardStatusMap.get(dataStoreType);
		return shardToStatusMap == null ? null : shardToStatusMap.get(shardId);
	}

	public Map<String, ShardStatus> getStatusMapForDataStore(DataStoreType dataStoreType)
	{
		Map<String, ShardStatus> statusMap =
		        new HashMap<String, ShardStatus>(dataStoreTypeToShardStatusMap.get(dataStoreType));
		return statusMap;
	}

	public Map<DataStoreType, Map<String, ShardStatus>> getStatusMap()
	{
		Map<DataStoreType, Map<String, ShardStatus>> statusMap =
		        new HashMap<DataStoreType, Map<String, ShardStatus>>(dataStoreTypeToShardStatusMap);
		return statusMap;
	}
}
