package com.flipkart.portkey.common.persistence.query;

import java.util.HashMap;
import java.util.Map;

public class SqlQuery
{
	private String query;
	private Map<String, Object> columnToValueMap = new HashMap<String, Object>();

	public String getQuery()
	{
		return query;
	}

	public void setQueryString(String query)
	{
		this.query = query;
	}

	public Map<String, Object> getColumnToValueMap()
	{
		return columnToValueMap;
	}

	public Object getValueFromColumn(String column)
	{
		return columnToValueMap.get(column);
	}

	public void setColumnToValueMap(Map<String, Object> columnToValueMap)
	{
		this.columnToValueMap = columnToValueMap;
	}

	public void addToColumnToValueMap(String column, Object value)
	{
		this.columnToValueMap.put(column, value);
	}
}
