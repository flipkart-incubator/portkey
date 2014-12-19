/**
 * 
 */
package com.flipkart.portkey.rdbms.metadata;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;

/**
 * @author santosh.p
 */
public class RdbmsTableMetaData
{
	private String databaseName;
	private String tableName;
	private String shardKey;
	private List<String> primaryKeys;
	private List<RdbmsField> rdbmsFieldList;
	private List<Field> fieldList;
	private List<String> jsonFields;
	private Map<String, String> fieldNameToRdbmsColumnMap;
	private Map<String, String> rdbmsColumnToFieldNameMap;
	private Map<String, Field> fieldNameToFieldMap;
	private List<String> jsonListFields;
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

	public List<RdbmsField> getRdbmsFieldList()
	{
		return rdbmsFieldList;
	}

	public void setRdbmsFieldList(List<RdbmsField> rdbmsFieldList)
	{
		this.rdbmsFieldList = rdbmsFieldList;
	}

	public void addToRdbmsFieldList(RdbmsField rdbmsField)
	{
		this.rdbmsFieldList.add(rdbmsField);
	}

	public void addToPrimaryKeys(String priamryKey)
	{
		this.primaryKeys.add(priamryKey);
	}

	public List<Field> getFieldList()
	{
		return fieldList;
	}

	public void setFieldList(List<Field> fieldList)
	{
		this.fieldList = fieldList;
	}

	public void addToFieldList(Field field)
	{
		this.fieldList.add(field);
	}

	public List<String> getJsonFields()
	{
		return jsonFields;
	}

	public void setJsonFields(List<String> jsonFields)
	{
		this.jsonFields = jsonFields;
	}

	public void addToJsonFields(String jsonField)
	{
		this.jsonFields.add(jsonField);
	}

	public Map<String, String> getFieldNameToRdbmsColumnMap()
	{
		return fieldNameToRdbmsColumnMap;
	}

	public void setFieldNameToRdbmsColumnMap(Map<String, String> fieldNameToSqlColumnMap)
	{
		this.fieldNameToRdbmsColumnMap = fieldNameToSqlColumnMap;
	}

	public void addToFieldNameToRdbmsColumnMap(String fieldName, String column)
	{
		this.fieldNameToRdbmsColumnMap.put(fieldName, column);
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
		this.fieldNameToRdbmsColumnMap.put(column, fieldName);
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

	public List<String> getJsonListFields()
	{
		return jsonListFields;
	}

	public void setJsonListFields(List<String> jsonListFields)
	{
		this.jsonListFields = jsonListFields;
	}

	public void addToJsonListFields(String jsonListField)
	{
		this.jsonListFields.add(jsonListField);
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
