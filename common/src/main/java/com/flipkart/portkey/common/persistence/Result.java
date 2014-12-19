/**
 * 
 */
package com.flipkart.portkey.common.persistence;

import java.util.HashMap;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.DataStoreType;

/**
 * @author santosh.p
 */
public class Result
{
	Entity entity;
	Map<DataStoreType, Integer> dataStoreToRowsUpdatedMap;

	public Result()
	{
		dataStoreToRowsUpdatedMap = new HashMap<DataStoreType, Integer>();
	}

	public Entity getEntity()
	{
		return entity;
	}

	public void setEntity(Entity entity)
	{
		this.entity = entity;
	}

	public Map<DataStoreType, Integer> getDataStoreToRowsUpdatedMap()
	{
		return dataStoreToRowsUpdatedMap;
	}

	public Integer getRowsUpdatedForDataStore(DataStoreType ds)
	{
		if (dataStoreToRowsUpdatedMap == null)
		{
			return null;
		}
		return dataStoreToRowsUpdatedMap.get(ds);
	}

	public void setDataStoreToRowsUpdatedMap(Map<DataStoreType, Integer> dataStoreToRowsUpdatedMap)
	{
		this.dataStoreToRowsUpdatedMap = dataStoreToRowsUpdatedMap;
	}

	public void setRowsUpdatedForDataStore(DataStoreType ds, Integer rowsUpdated)
	{
		this.dataStoreToRowsUpdatedMap.put(ds, rowsUpdated);
	}
}
