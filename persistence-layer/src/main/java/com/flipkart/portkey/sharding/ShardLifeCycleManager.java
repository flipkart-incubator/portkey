/**
 * 
 */
package com.flipkart.portkey.sharding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.sharding.ShardLifeCycleManagerInterface;
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
	Config cfg = new Config();
	List<DataStoreType> dataStoreTypesList;
	IMap<DataStoreType, Map<String, ShardStatus>> shardStatusMap;

	private void initialize()
	{
		logger.info("initializing ShardLifeCycleManager");
		HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
		shardStatusMap = instance.getMap("liveShards");
		for (DataStoreType dataStoreType : dataStoreTypesList)
		{
			Map<String, ShardStatus> innerMap = new HashMap<String, ShardStatus>();
			shardStatusMap.put(dataStoreType, innerMap);
		}
		logger.info("initialization complete");
	}

	public ShardLifeCycleManager(List<DataStoreType> dataStores)
	{
		this.dataStoreTypesList = dataStores;
		initialize();
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.sharding.ShardLifeCycleManagerInterface#setShardStatus(com.flipkart.portkey.common
	 * .enumeration.DataStoreType, java.lang.String, com.flipkart.portkey.common.enumeration.ShardStatus)
	 */
	public void setShardStatus(DataStoreType dataStoreType, String shardId, ShardStatus shardStatus)
	{
		logger.info("setting shard status datastoretype=" + dataStoreType + " shardId=" + shardId + " shardStatus="
		        + shardStatus);
		// TODO: handle NullPointerException
		Map<String, ShardStatus> statusMapForDS = shardStatusMap.get(dataStoreType);
		statusMapForDS.put(shardId, shardStatus);
		shardStatusMap.put(dataStoreType, statusMapForDS);
		logger.info("value set=" + shardStatusMap.get(dataStoreType).get(shardId));
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.sharding.ShardLifeCycleManagerInterface#getShardListForStatus(com.flipkart.portkey
	 * .common.enumeration.DataStoreType, com.flipkart.portkey.common.enumeration.ShardStatus)
	 */
	public List<String> getShardListForStatus(DataStoreType dataStoreType, ShardStatus shardStatus)
	{
		logger.info("generating shard list with status=" + shardStatus + " for datastoretype=" + dataStoreType);
		Map<String, ShardStatus> statusMapForDS = shardStatusMap.get(dataStoreType);
		if (statusMapForDS == null)
		{
			logger.info("no shards are registered for datastore type=" + dataStoreType);
			logger.info("returning null");
			return null;
		}
		logger.info("number of shards registered for " + dataStoreType + "=" + statusMapForDS.size());
		List<String> shardList = new ArrayList<String>();
		for (String shardId : statusMapForDS.keySet())
		{
			if (statusMapForDS.get(shardId) == shardStatus)
			{
				shardList.add(shardId);
			}
		}
		logger.info("shard list=" + shardList);
		return shardList.size() > 0 ? shardList : null;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.sharding.ShardLifeCycleManagerInterface#getShardStatus(com.flipkart.portkey.common
	 * .enumeration.DataStoreType, java.lang.String)
	 */
	public ShardStatus getShardStatus(DataStoreType dataStoreType, String shardId)
	{
		// TODO: handle NullPointerException
		return shardStatusMap.get(dataStoreType).get(shardId);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.sharding.ShardLifeCycleManagerInterface#getStatusForDataStore(com.flipkart.portkey
	 * .common.enumeration.DataStoreType)
	 */
	public Map<String, ShardStatus> getStatusMapForDataStore(DataStoreType dataStoreType)
	{
		Map<String, ShardStatus> statusMap = new HashMap<String, ShardStatus>(shardStatusMap.get(dataStoreType));
		return statusMap;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.sharding.ShardLifeCycleManagerInterface#getStatus()
	 */
	public Map<DataStoreType, Map<String, ShardStatus>> getStatus()
	{
		Map<DataStoreType, Map<String, ShardStatus>> statusMap =
		        new HashMap<DataStoreType, Map<String, ShardStatus>>(shardStatusMap);
		return statusMap;
	}
}
