/**
 * 
 */
package com.flipkart.portkey.rdbms.metadata;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.serializer.Serializer;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;

/**
 * @author santosh.p
 */
public class RdbmsTableMetaData
{
	private String databaseName;
	private String tableName;
	private String shardKey;
	private List<String> primaryKeys = new ArrayList<String>();
	private Map<String, RdbmsField> fieldNameToRdbmsFieldMap = new HashMap<String, RdbmsField>();
	private Map<String, Serializer> fieldNameToSerializerMap = new HashMap<String, Serializer>();
	private Map<String, String> fieldNameToRdbmsColumnMap = new HashMap<String, String>();
	private Map<String, String> rdbmsColumnToFieldNameMap = new HashMap<String, String>();
	private Map<String, Field> fieldNameToFieldMap = new HashMap<String, Field>();
	private String insertQuery;
	private String updateByPkQuery;

	public String getDatabaseName()
	{
		return databaseName;
	}

	public void setDatabaseName(String databaseName)
	{
		this.databaseName = databaseName;
	}

	public String getTableName()
	{
		return tableName;
	}

	public void setTableName(String tableName)
	{
		this.tableName = tableName;
	}

	public String getShardKey()
	{
		return shardKey;
	}

	public void setShardKey(String shardKey)
	{
		this.shardKey = shardKey;
	}

	public List<String> getPrimaryKeys()
	{
		return primaryKeys;
	}

	public void setPrimaryKeys(List<String> primaryKeys)
	{
		this.primaryKeys = primaryKeys;
	}

	public RdbmsField getRdbmsField(String fieldName)
	{
		return fieldNameToRdbmsFieldMap.get(fieldName);
	}

	public void addRdbmsField(String fieldName, RdbmsField rdbmsField)
	{
		this.fieldNameToRdbmsFieldMap.put(fieldName, rdbmsField);
	}

	public void addToPrimaryKeys(String priamryKey)
	{
		this.primaryKeys.add(priamryKey);
	}

	public List<Field> getFieldList()
	{
		return new ArrayList<Field>(fieldNameToFieldMap.values());
	}

	public Map<String, String> getFieldNameToRdbmsColumnMap()
	{
		return fieldNameToRdbmsColumnMap;
	}

	public String getRdbmsColumnFromFieldName(String fieldName)
	{
		return fieldNameToRdbmsColumnMap.get(fieldName);
	}

	public void setFieldNameToRdbmsColumnMap(Map<String, String> fieldNameToSqlColumnMap)
	{
		this.fieldNameToRdbmsColumnMap = fieldNameToSqlColumnMap;
	}

	public void addToFieldNameToRdbmsColumnMap(String fieldName, String column)
	{
		this.fieldNameToRdbmsColumnMap.put(fieldName, column);
	}

	public Serializer getSerializer(String fieldName)
	{
		return fieldNameToSerializerMap.get(fieldName);
	}

	public void setSerializer(String fieldName, Serializer serializer)
	{
		this.fieldNameToSerializerMap.put(fieldName, serializer);
	}

	public Map<String, String> getRdbmsColumnToFieldNameMap()
	{
		return rdbmsColumnToFieldNameMap;
	}

	public void setRdbmsColumnToFieldNameMap(Map<String, String> sqlColumnToFieldNameMap)
	{
		this.rdbmsColumnToFieldNameMap = sqlColumnToFieldNameMap;
	}

	public void addToRdbmsColumnToFieldNameMap(String column, String fieldName)
	{
		this.rdbmsColumnToFieldNameMap.put(column, fieldName);
	}

	public Map<String, Field> getFieldNameToFieldMap()
	{
		return fieldNameToFieldMap;
	}

	public void setFieldNameToFieldMap(Map<String, Field> fieldNameToFieldMap)
	{
		this.fieldNameToFieldMap = fieldNameToFieldMap;
	}

	public void addToFieldNameToFieldMap(String fieldName, Field field)
	{
		this.fieldNameToFieldMap.put(fieldName, field);
	}

	public String getInsertQuery()
	{
		return insertQuery;
	}

	public void setInsertQuery(String insertQuery)
	{
		this.insertQuery = insertQuery;
	}

	public String getUpdateByPkQuery()
	{
		return updateByPkQuery;
	}

	public void setUpdateByPkQuery(String updateByPkQuery)
	{
		this.updateByPkQuery = updateByPkQuery;
	}
}
