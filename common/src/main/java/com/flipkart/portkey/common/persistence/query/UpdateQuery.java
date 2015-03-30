package com.flipkart.portkey.common.persistence.query;

import java.util.HashMap;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;

public class UpdateQuery
{
	Class<? extends Entity> clazz;
	Map<String, Object> updateValuesMap = new HashMap<String, Object>();
	Map<String, Object> criteria = new HashMap<String, Object>();

	public Class<? extends Entity> getClazz()
	{
		return clazz;
	}

	public void setClazz(Class<? extends Entity> clazz)
	{
		this.clazz = clazz;
	}

	public Map<String, Object> getUpdateValuesMap()
	{
		return updateValuesMap;
	}

	public void setUpdateValuesMap(Map<String, Object> updateValuesMap)
	{
		this.updateValuesMap = updateValuesMap;
	}

	public void addToUpdateValuesMap(String key, Object value)
	{
		this.updateValuesMap.put(key, value);
	}

	public Map<String, Object> getCriteria()
	{
		return criteria;
	}

	public void setCriteria(Map<String, Object> criteria)
	{
		this.criteria = criteria;
	}

	public void addCriteria(String key, Object value)
	{
		this.criteria.put(key, value);
	}
}
