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

	public String getString(String key)
	{
		if (map.get(key) != null)
		{
			return map.get(key).toString();
		}
		return null;
	}

	public Short getShort(String key)
	{
		String value = getString(key);
		if (value != null)
		{
			return Short.valueOf(value);
		}
		return null;
	}

	public Integer getInt(String key)
	{
		String value = getString(key);
		if (value != null)
		{
			return Integer.valueOf(value);
		}
		return null;
	}

	public Long getLong(String key)
	{
		String value = getString(key);
		if (value != null)
		{
			return Long.valueOf(value);
		}
		return null;
	}

	public Float getFloat(String key)
	{
		String value = getString(key);
		if (value != null)
		{
			return Float.valueOf(value);
		}
		return null;
	}

	public Double getDouble(String key)
	{
		String value = getString(key);
		if (value != null)
		{
			return Double.valueOf(value);
		}
		return null;
	}

	public Boolean getBoolean(String key)
	{
		String value = getString(key);
		if (value != null)
		{
			return Boolean.valueOf(value);
		}
		return null;
	}

	public Object get(String key)
	{
		return map.get(key);
	}

}
