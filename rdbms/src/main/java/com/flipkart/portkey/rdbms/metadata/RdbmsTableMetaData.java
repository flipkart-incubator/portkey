/**
 * 
 */
package com.flipkart.portkey.rdbms.metadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
public class RdbmsTableMetaData extends RdbmsMetaData
{

	private String tableName;
	private String shardKeyFieldName;
	private List<String> primaryKeysList = new ArrayList<String>();
	private Map<String, RdbmsField> fieldNameToRdbmsFieldMap = new LinkedHashMap<String, RdbmsField>();
	private Map<String, String> fieldNameToDefaultInsertValueMap = new LinkedHashMap<String, String>();
	private Map<String, String> fieldNameToDefaultUpdateValueMap = new LinkedHashMap<String, String>();
	private String insertQuery;
	private String updateByPkQuery;
	private String upsertQuery;

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

	public Map<String, String> getFieldNameToDefaultInsertValueMap()
	{
		return fieldNameToDefaultInsertValueMap;
	}

	public String getDefaultInsertValueFromFieldName(String fieldName)
	{
		return fieldNameToDefaultInsertValueMap.get(fieldName);
	}

	public void setFieldNameToDefaultInsertValueMap(Map<String, String> fieldNameToDefaultInsertValueMap)
	{
		this.fieldNameToDefaultInsertValueMap = fieldNameToDefaultInsertValueMap;
	}

	public void addToFieldNameToDefaultInsertValueMap(String fieldName, String defaultInsertValue)
	{
		this.fieldNameToDefaultInsertValueMap.put(fieldName, defaultInsertValue);
	}

	public Map<String, String> getFieldNameToDefaultUpdateValueMap()
	{
		return fieldNameToDefaultUpdateValueMap;
	}

	public String getDefaultUpdateValueFromFieldName(String fieldName)
	{
		return fieldNameToDefaultUpdateValueMap.get(fieldName);
	}

	public void setFieldNameToDefaultUpdateValueMap(Map<String, String> fieldNameToDefaultUpdateValueMap)
	{
		this.fieldNameToDefaultUpdateValueMap = fieldNameToDefaultUpdateValueMap;
	}

	public void addToFieldNameToDefaultUpdateValueMap(String fieldName, String defaultUpdateValue)
	{
		this.fieldNameToDefaultUpdateValueMap.put(fieldName, defaultUpdateValue);
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
