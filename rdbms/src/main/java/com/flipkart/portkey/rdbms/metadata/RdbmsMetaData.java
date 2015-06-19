package com.flipkart.portkey.rdbms.metadata;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.serializer.Serializer;

public class RdbmsMetaData
{
	private String databaseName;
	private String shardKeyFieldName;
	private Map<String, Serializer> fieldNameToSerializerMap = new LinkedHashMap<String, Serializer>();
	private Map<String, String> fieldNameToColumnNameMap = new LinkedHashMap<String, String>();
	private Map<String, String> columnNameToFieldNameMap = new LinkedHashMap<String, String>();
	private Map<String, Field> fieldNameToFieldMap = new LinkedHashMap<String, Field>();

	public String getDatabaseName()
	{
		return databaseName;
	}

	public void setDatabaseName(String databaseName)
	{
		this.databaseName = databaseName;
	}

	public String getShardKeyFieldName()
	{
		return shardKeyFieldName;
	}

	public void setShardKeyFieldName(String shardKeyFieldName)
	{
		this.shardKeyFieldName = shardKeyFieldName;
	}

	public Map<String, Serializer> getFieldNameToSerializerMap()
	{
		return fieldNameToSerializerMap;
	}

	public Serializer getSerializerFromFieldName(String fieldName)
	{
		return fieldNameToSerializerMap.get(fieldName);
	}

	public void setFieldNameToSerializerMap(Map<String, Serializer> fieldNameToSerializerMap)
	{
		this.fieldNameToSerializerMap = fieldNameToSerializerMap;
	}

	public void addToFieldNameToSerializerMap(String fieldName, Serializer serializer)
	{
		this.fieldNameToSerializerMap.put(fieldName, serializer);
	}

	public Map<String, String> getFieldNameToColumnNameMap()
	{
		return fieldNameToColumnNameMap;
	}

	public String getColumnNameFromFieldName(String fieldName)
	{
		return fieldNameToColumnNameMap.get(fieldName);
	}

	public void setFieldNameToColumnNameMap(Map<String, String> fieldNameToColumnNameMap)
	{
		this.fieldNameToColumnNameMap = fieldNameToColumnNameMap;
	}

	public void addToFieldNameToColumnNameMap(String fieldName, String columnName)
	{
		this.fieldNameToColumnNameMap.put(fieldName, columnName);
	}

	public Map<String, String> getColumnNameToFieldNameMap()
	{
		return columnNameToFieldNameMap;
	}

	public String getFieldNameFromColumnName(String columnName)
	{
		return this.columnNameToFieldNameMap.get(columnName);
	}

	public void setColumnNameToFieldNameMap(Map<String, String> columnNameToFieldNameMap)
	{
		this.columnNameToFieldNameMap = columnNameToFieldNameMap;
	}

	public void addToColumnNameToFieldNameMap(String columnName, String fieldName)
	{
		this.columnNameToFieldNameMap.put(columnName, fieldName);
	}

	public Map<String, Field> getFieldNameToFieldMap()
	{
		return fieldNameToFieldMap;
	}

	public Field getFieldFromFieldName(String fieldName)
	{
		return this.fieldNameToFieldMap.get(fieldName);
	}

	public void setFieldNameToFieldMap(Map<String, Field> fieldNameToFieldMap)
	{
		this.fieldNameToFieldMap = fieldNameToFieldMap;
	}

	public void addToFieldNameToFieldMap(String fieldName, Field field)
	{
		this.fieldNameToFieldMap.put(fieldName, field);
	}

	public List<Field> getFieldsList()
	{
		return new ArrayList<Field>(fieldNameToFieldMap.values());
	}
}
