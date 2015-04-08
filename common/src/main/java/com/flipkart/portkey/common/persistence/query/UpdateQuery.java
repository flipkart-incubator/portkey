package com.flipkart.portkey.common.persistence.query;

import java.util.HashMap;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;

public class UpdateQuery implements PortKeyQuery
{
	Class<? extends Entity> clazz;
	Map<String, Object> updateFieldNameToValueeMap = new HashMap<String, Object>();
	Map<String, Object> criteriaFieldNameToValueMap = new HashMap<String, Object>();

	public Class<? extends Entity> getClazz()
	{
		return clazz;
	}

	public void setClazz(Class<? extends Entity> clazz)
	{
		this.clazz = clazz;
	}

	public Map<String, Object> getUpdateFieldNameToValueMap()
	{
		return updateFieldNameToValueeMap;
	}

	public void setUpdateFieldNameToValueMap(Map<String, Object> updateFieldNameToValueMap)
	{
		this.updateFieldNameToValueeMap = updateFieldNameToValueMap;
	}

	public void addToUpdateFieldNameToValueMap(String key, Object value)
	{
		this.updateFieldNameToValueeMap.put(key, value);
	}

	public Map<String, Object> getCriteriaFieldNameToValueMap()
	{
		return criteriaFieldNameToValueMap;
	}

	public void setCriteriaFieldNameToValueMap(Map<String, Object> criteriaFieldNameToValueMap)
	{
		this.criteriaFieldNameToValueMap = criteriaFieldNameToValueMap;
	}

	public void addToCriteriaFieldNameToValueMap(String fieldName, Object value)
	{
		this.criteriaFieldNameToValueMap.put(fieldName, value);
	}
}
