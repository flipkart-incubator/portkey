/**
 * 
 */
package com.flipkart.portkey.metadata.datastore;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.metadata.annotation.RdbmsField;

/**
 * @author santosh.p
 */
public class RdbmsTableMetaData
{
	private String databaseName;
	private String tableName;
	private List<String> primaryKeys;
	private List<RdbmsField> rdbmsFieldList;
	private List<Field> fieldList;
	private List<String> jsonFields;
	private Map<String, String> fieldNameToRdbmsColumnMap;
	private Map<String, String> rdbmsColumnToFieldNameMap;
	private List<String> jsonListFields;

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
}
