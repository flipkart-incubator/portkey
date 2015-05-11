package com.flipkart.portkey.rdbms.persistence;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.rdbms.mapper.RdbmsMapper;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;
import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;

public class RdbmsHelper
{
	public static Map<String, Object> generateColumnToValueMap(Entity obj, RdbmsTableMetaData metaData)
	{
		List<Field> fieldsList = metaData.getFieldsList();
		Map<String, Object> columnToValueMap = new HashMap<String, Object>();
		for (Field field : fieldsList)
		{
			RdbmsField rdbmsField = field.getAnnotation(RdbmsField.class);
			if (rdbmsField != null)
			{
				Object value;
				value = RdbmsMapper.get(obj, field.getName());
				columnToValueMap.put(rdbmsField.columnName(), value);
			}
		}
		return columnToValueMap;
	}

	public static <T extends Entity> Map<String, Object> generateColumnToValueMap(Class<T> clazz,
	        Map<String, Object> fieldNameToValueMap)
	{
		Map<String, Object> columnToValueMap = new HashMap<String, Object>();
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		for (String fieldName : fieldNameToValueMap.keySet())
		{
			String columnName = metaData.getColumnNameFromFieldName(fieldName);
			Object valueBeforeSerialization = fieldNameToValueMap.get(fieldName);
			Object value = RdbmsMapper.get(clazz, fieldName, valueBeforeSerialization);
			columnToValueMap.put(columnName, value);
		}
		return columnToValueMap;
	}

	public static List<String> generateColumnsListFromFieldNamesList(RdbmsTableMetaData metaData,
	        List<String> fieldNamesList)
	{
		List<String> columnsList = new ArrayList<String>();
		for (String fieldName : fieldNamesList)
		{
			columnsList.add(metaData.getColumnNameFromFieldName(fieldName));
		}
		return columnsList;
	}
}
