package com.flipkart.portkey.persistence.helper;

import java.util.Map;

import com.flipkart.portkey.common.persistence.Row;

public class PortKeyHelper
{

	public static Row mapToRow(Map<String, Object> map)
	{
		Row row = new Row();
		row.setMap(map);
		return row;
	}

}
