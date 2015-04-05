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

/**
 * @author santosh.p
 */
public class SimpleShardLifeCycleManager implements ShardLifeCycleManagerInterface
{
	private static final Logger logger = Logger.getLogger(SimpleShardLifeCycleManager.class);
	Config cfg = new Config();
	List<DataStoreType> dataStoreTypesList;
	Map<DataStoreType, Map<String, ShardStatus>> shardStatusMap;

	private void initialize()
	{
		logger.info("initializing ShardLifeCycleManager");
		shardStatusMap = new HashMap<DataStoreType, Map<String, ShardStatus>>();
		for (DataStoreType dataStoreType : dataStoreTypesList)
		{
			Map<String, ShardStatus> innerMap = new HashMap<String, ShardStatus>();
			shardStatusMap.put(dataStoreType, innerMap);
		}
		logger.info("initialization complete");
	}

	public SimpleShardLifeCycleManager(List<DataStoreType> dataStores)
	{
		this.dataStoreTypesList = dataStores;
		initialize();
	}

	public void setShardStatus(DataStoreType dataStoreType, String shardId, ShardStatus shardStatus)
	{
		logger.info("setting shard status datastoretype=" + dataStoreType + " shardId=" + shardId + " shardStatus="
		        + shardStatus);
		// TODO: handle NullPointerException
		Map<String, ShardStatus> statusMapForDS = shardStatusMap.get(dataStoreType);
		statusMapForDS.put(shardId, shardStatus);
		shardStatusMap.put(dataStoreType, statusMapForDS);
	}

	public List<String> getShardListForStatus(DataStoreType dataStoreType, ShardStatus shardStatus)
	{
		logger.debug("generating shard list with status=" + shardStatus + " for datastoretype=" + dataStoreType);
		Map<String, ShardStatus> statusMapForDS = shardStatusMap.get(dataStoreType);
		if (statusMapForDS == null)
		{
			logger.debug("no shards are registered for datastore type=" + dataStoreType);
			return null;
		}
		logger.debug("number of shards registered for " + dataStoreType + "=" + statusMapForDS.size());
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

	public ShardStatus getShardStatus(DataStoreType dataStoreType, String shardId)
	{
		// TODO: handle NullPointerException
		return shardStatusMap.get(dataStoreType).get(shardId);
	}

	public Map<String, ShardStatus> getStatusMapForDataStore(DataStoreType dataStoreType)
	{
		Map<String, ShardStatus> statusMap = new HashMap<String, ShardStatus>(shardStatusMap.get(dataStoreType));
		return statusMap;
	}

	public Map<DataStoreType, Map<String, ShardStatus>> getStatusMap()
	{
		Map<DataStoreType, Map<String, ShardStatus>> statusMap =
		        new HashMap<DataStoreType, Map<String, ShardStatus>>(shardStatusMap);
		return statusMap;
	}
}
