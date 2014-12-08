/**
 * 
 */
package com.flipkart.portkey.metadata.datastore;

import java.util.List;
import java.util.Map;

/**
 * @author santosh.p
 */
public class RedisTableMetaData
{
	int database;
	String primaryKey;
	List<String> secondaryKeys;
	private Map<String, String> fieldNameToAttributeNameMap;
	private Map<String, String> attributeNameToFieldNameMap;
	private List<String> jsonFields;
	private List<String> jsonListFields;

	public int getDatabase()
	{
		return database;
	}

	public void setDatabase(int database)
	{
		this.database = database;
	}

	public String getPrimaryKey()
	{
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey)
	{
		this.primaryKey = primaryKey;
	}

	public List<String> getSecondaryKeys()
	{
		return secondaryKeys;
	}

	public void setSecondaryKeys(List<String> secondaryKeys)
	{
		this.secondaryKeys = secondaryKeys;
	}

	public void addToSecondaryKeys(String secondaryKey)
	{
		this.secondaryKeys.add(secondaryKey);
	}

	public Map<String, String> getFieldNameToAttributeNameMap()
	{
		return fieldNameToAttributeNameMap;
	}

	public void setFieldNameToAttributeNameMap(Map<String, String> fieldNameToAttributeNameMap)
	{
		this.fieldNameToAttributeNameMap = fieldNameToAttributeNameMap;
	}

	public void addToFieldNameToAttributeNameMap(String fieldName, String attributeName)
	{
		this.fieldNameToAttributeNameMap.put(fieldName, attributeName);
	}

	public Map<String, String> getRedisColumnToFieldNameMap()
	{
		return attributeNameToFieldNameMap;
	}

	public void setRedisColumnToFieldNameMap(Map<String, String> attributeNameToFieldNameMap)
	{
		this.attributeNameToFieldNameMap = attributeNameToFieldNameMap;
	}

	public void addToAttributeNameToFieldNameMap(String attributeName, String fieldName)
	{
		this.fieldNameToAttributeNameMap.put(attributeName, fieldName);
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
		this.jsonFields.add(jsonListField);
	}
}
