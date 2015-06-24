package com.flipkart.portkey.common.persistence;

import java.util.LinkedHashMap;
import java.util.Map;

public class Row
{
	Map<String, Object> map;

	public Row()
	{
		map = new LinkedHashMap<String, Object>();
	}

	public void setMap(Map<String, Object> map)
	{
		this.map = map;
	}

	public Byte getByte(String key)
	{
		return (Byte) map.get(key);
	}

	public Short getShort(String key)
	{
		return (Short) map.get(key);
	}

	public Integer getInt(String key)
	{
		return (Integer) map.get(key);
	}

	public Long getLong(String key)
	{
		return (Long) map.get(key);
	}

	public Float getFloat(String key)
	{
		return (Float) map.get(key);
	}

	public Double getDouble(String key)
	{
		return (Double) map.get(key);
	}

	public Boolean getBoolean(String key)
	{
		return (Boolean) map.get(key);
	}

	public String getString(String key)
	{
		return (String) map.get(key);
	}

	public Object get(String key)
	{
		return map.get(key);
	}

}
