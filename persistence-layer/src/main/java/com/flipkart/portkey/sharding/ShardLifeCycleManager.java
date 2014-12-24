/**
 * 
 */
package com.flipkart.portkey.sharding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	Config cfg = new Config();
	List<DataStoreType> dataStoreTypesList;
	IMap<DataStoreType, IMap<String, ShardStatus>> shardStatusMap;

	private void initialize()
	{
		HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
		shardStatusMap = instance.getMap("liveShards");
		for (DataStoreType ds : dataStoreTypesList)
		{
			IMap<String, ShardStatus> innerMap = instance.getMap(ds.toString());
			shardStatusMap.put(ds, innerMap);
		}
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
	public void setShardStatus(DataStoreType ds, String shardId, ShardStatus shardStatus)
	{
		// TODO: handle NullPointerException
		shardStatusMap.get(ds).set(shardId, shardStatus);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.sharding.ShardLifeCycleManagerInterface#getShardListForStatus(com.flipkart.portkey
	 * .common.enumeration.DataStoreType, com.flipkart.portkey.common.enumeration.ShardStatus)
	 */
	public List<String> getShardListForStatus(DataStoreType ds, ShardStatus shardStatus)
	{
		IMap<String, ShardStatus> statusMapForDS = shardStatusMap.get(ds);
		if (statusMapForDS == null)
		{
			return null;
		}
		List<String> shardList = new ArrayList<String>();
		for (String shardId : statusMapForDS.keySet())
		{
			if (statusMapForDS.get(shardId) == shardStatus)
			{
				shardList.add(shardId);
			}
		}
		return shardList.size() > 0 ? shardList : null;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.sharding.ShardLifeCycleManagerInterface#getShardStatus(com.flipkart.portkey.common
	 * .enumeration.DataStoreType, java.lang.String)
	 */
	public ShardStatus getShardStatus(DataStoreType ds, String shardId)
	{
		// TODO: handle NullPointerException
		return shardStatusMap.get(ds).get(shardId);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.common.sharding.ShardLifeCycleManagerInterface#getStatusForDataStore(com.flipkart.portkey
	 * .common.enumeration.DataStoreType)
	 */
	public Map<String, ShardStatus> getStatusMapForDataStore(DataStoreType ds)
	{
		Map<String, ShardStatus> statusMap = new HashMap<String, ShardStatus>(shardStatusMap.get(ds));
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
