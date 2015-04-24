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
 *         The class stores metadata for a Pojo ( and corresponding rdbms table).
 *         Naming conventions used are as follows:
 *         fieldName - Name of the field in pojo
 *         field - Object of class java.lang.reflect.Field
 *         rdbmsField - Object of Annotation type com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField
 *         columnName - Name of column in Rdbms table
 */
public class RdbmsTableMetaData
{
	private String databaseName;
	private String tableName;
	private String shardKeyFieldName;
	private List<String> primaryKeysList = new ArrayList<String>();
	private Map<String, RdbmsField> fieldNameToRdbmsFieldMap = new HashMap<String, RdbmsField>();
	private Map<String, Serializer> fieldNameToSerializerMap = new HashMap<String, Serializer>();
	private Map<String, String> fieldNameToColumnNameMap = new HashMap<String, String>();
	private Map<String, String> columnNameToFieldNameMap = new HashMap<String, String>();
	private Map<String, Field> fieldNameToFieldMap = new HashMap<String, Field>();
	private Map<String, String> fieldNameToDefaultValueMap = new HashMap<String, String>();
	private String insertQuery;
	private String updateByPkQuery;
	private String upsertQuery;

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

	public String getShardKeyFieldName()
	{
		return shardKeyFieldName;
	}

	public void setShardKeyFieldName(String shardKey)
	{
		this.shardKeyFieldName = shardKey;
	}

	public List<String> getPrimaryKeysList()
	{
		return primaryKeysList;
	}

	public void setPrimaryKeysList(List<String> primaryKeys)
	{
		this.primaryKeysList = primaryKeys;
	}

	public RdbmsField getRdbmsFieldFromFieldName(String fieldName)
	{
		return fieldNameToRdbmsFieldMap.get(fieldName);
	}

	public void addToFieldNameToRdbmsFieldMap(String fieldName, RdbmsField rdbmsField)
	{
		this.fieldNameToRdbmsFieldMap.put(fieldName, rdbmsField);
	}

	public void addPrimaryKey(String priamryKey)
	{
		this.primaryKeysList.add(priamryKey);
	}

	public List<Field> getFieldsList()
	{
		return new ArrayList<Field>(fieldNameToFieldMap.values());
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

	public void addToFieldNameToColumnNameMap(String fieldName, String column)
	{
		this.fieldNameToColumnNameMap.put(fieldName, column);
	}

	public Serializer getSerializerFromFieldName(String fieldName)
	{
		return fieldNameToSerializerMap.get(fieldName);
	}

	public void addToFieldNameToSerializerMap(String fieldName, Serializer serializer)
	{
		this.fieldNameToSerializerMap.put(fieldName, serializer);
	}

	public Map<String, String> getColumnNameToFieldNameMap()
	{
		return columnNameToFieldNameMap;
	}

	public void setColumnNameToFieldNameMap(Map<String, String> columnNameToFieldNameMap)
	{
		this.columnNameToFieldNameMap = columnNameToFieldNameMap;
	}

	public void addToColumnNameToFieldNameMap(String column, String fieldName)
	{
		this.columnNameToFieldNameMap.put(column, fieldName);
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

	public Map<String, String> getFieldNameToDefaultValueMap()
	{
		return fieldNameToDefaultValueMap;
	}

	public String getDefaultValueFromFieldName(String fieldName)
	{
		return fieldNameToDefaultValueMap.get(fieldName);
	}

	public void setFieldNameToDefaultValueMap(Map<String, String> fieldNameToDefaultValueMap)
	{
		this.fieldNameToDefaultValueMap = fieldNameToDefaultValueMap;
	}

	public void addToFieldNameToDefaultValueMap(String fieldName, String defaultValue)
	{
		this.fieldNameToDefaultValueMap.put(fieldName, defaultValue);
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

	public String getUpsertQuery()
	{
		return upsertQuery;
	}

	public void setUpsertQuery(String upsertQuery)
	{
		this.upsertQuery = upsertQuery;
	}
}
