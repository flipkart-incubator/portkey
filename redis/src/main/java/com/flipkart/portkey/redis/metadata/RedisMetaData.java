/**
 * 
 */
package com.flipkart.portkey.redis.metadata;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.redis.metadata.annotation.RedisField;

/**
 * @author santosh.p
 */
public class RedisMetaData
{
	private int database;
	private String shardKey;
	private String primaryKeyPattern;
	private List<String> primaryKeyAttributes;
	private String multiLevelDataStructure = "HASH";
	private List<String> secondaryKeyPatterns;
	private List<List<String>> secondaryKeyAttributesList;
	private Map<String, List<String>> secondaryKeyPatternToAttributeList;
	private List<RedisField> redisFields;
	private List<Field> fields;
	private List<String> jsonFields;
	private List<String> jsonListFields;
	private Map<String, String> fieldNameToAttributeMap;
	private Map<String, String> attributeToFieldNameMap;
	private Map<String, Field> fieldNameToFieldMap;
	private Map<String, RedisField> fieldNameToRedisFieldMap;

	public int getDatabase()
	{
		return database;
	}

	public void setDatabase(int database)
	{
		this.database = database;
	}

	public String getShardKey()
	{
		return shardKey;
	}

	public void setShardKey(String shardKey)
	{
		this.shardKey = shardKey;
	}

	public String getPrimaryKeyPattern()
	{
		return primaryKeyPattern;
	}

	public void setPrimaryKeyPattern(String parsedKeyPattern)
	{
		this.primaryKeyPattern = parsedKeyPattern;
	}

	public List<String> getPrimaryKeyAttributes()
	{
		return primaryKeyAttributes;
	}

	public void setPrimaryKeyAttributes(List<String> primaryKeyAttributes)
	{
		this.primaryKeyAttributes = primaryKeyAttributes;
	}

	public String getMultiLevelDataStructure()
	{
		return multiLevelDataStructure;
	}

	public void setMultiLevelDataStructure(String multiLevelDataStructure)
	{
		this.multiLevelDataStructure = multiLevelDataStructure;
	}

	public List<String> getSecondaryKeyPatterns()
	{
		return secondaryKeyPatterns;
	}

	public void setSecondaryKeyPatterns(List<String> secondaryKeyPatterns)
	{
		this.secondaryKeyPatterns = secondaryKeyPatterns;
	}

	public void addToSecondaryKeyPatterns(String secondaryKeyPattern)
	{
		this.secondaryKeyPatterns.add(secondaryKeyPattern);
	}

	public List<List<String>> getSecondaryKeyAttributesList()
	{
		return secondaryKeyAttributesList;
	}

	public void setSecondaryKeyAttributesList(List<List<String>> secondaryKeyAttributesList)
	{
		this.secondaryKeyAttributesList = secondaryKeyAttributesList;
	}

	public void addToSecondaryKeyAttributesList(List<String> secondaryKeyAttributes)
	{
		this.secondaryKeyAttributesList.add(secondaryKeyAttributes);
	}

	public Map<String, List<String>> getSecondaryKeyPatternToAttributeList()
	{
		return secondaryKeyPatternToAttributeList;
	}

	public List<String> getAttributeListFromKeyPattern(String keyPattern)
	{
		if (secondaryKeyPatternToAttributeList == null)
		{
			return null;
		}
		return secondaryKeyPatternToAttributeList.get(keyPattern);
	}

	public void setSecondaryKeyPatternToAttributeList(Map<String, List<String>> secondaryKeyPatternToAttributeList)
	{
		this.secondaryKeyPatternToAttributeList = secondaryKeyPatternToAttributeList;
	}

	public void addToSecondaryKeyPatternToAttributeList(String keyPattern, List<String> attributeList)
	{
		this.secondaryKeyPatternToAttributeList.put(keyPattern, attributeList);
	}

	public List<RedisField> getRedisFields()
	{
		return redisFields;
	}

	public void setRedisFields(List<RedisField> redisFields)
	{
		this.redisFields = redisFields;
	}

	public void addToRedisFieldList(RedisField redisField)
	{
		this.redisFields.add(redisField);
	}

	public List<Field> getFields()
	{
		return fields;
	}

	public void setFields(List<Field> fields)
	{
		this.fields = fields;
	}

	public void addToFields(Field field)
	{
		this.fields.add(field);
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
		this.jsonListFields.add(jsonListField);
	}

	public Map<String, String> getFieldNameToAttributeMap()
	{
		return fieldNameToAttributeMap;
	}

	public String getAttributeFromFieldName(String fieldName)
	{
		if (fieldNameToAttributeMap == null)
		{
			return null;
		}
		return fieldNameToAttributeMap.get(fieldName);
	}

	public void setFieldNameToAttributeMap(Map<String, String> fieldNameToAttributeMap)
	{
		this.fieldNameToAttributeMap = fieldNameToAttributeMap;
	}

	public void addToFieldNameToAttributeMap(String fieldName, String attribute)
	{
		this.fieldNameToAttributeMap.put(fieldName, attribute);
	}

	public Map<String, String> getAttributeToFieldNameMap()
	{
		return attributeToFieldNameMap;
	}

	public String getFieldNameFromAttribute(String attribute)
	{
		if (attributeToFieldNameMap == null)
		{
			return null;
		}
		return attributeToFieldNameMap.get(attribute);
	}

	public void setAttributeToFieldNameMap(Map<String, String> attributeToFieldNameMap)
	{
		this.attributeToFieldNameMap = attributeToFieldNameMap;
	}

	public void addToAttributeToFieldNameMap(String attribute, String fieldName)
	{
		this.attributeToFieldNameMap.put(attribute, fieldName);
	}

	public Map<String, Field> getFieldNameToFieldMap()
	{
		return fieldNameToFieldMap;
	}

	public Field getFieldFromFieldName(String fieldName)
	{
		if (fieldNameToFieldMap == null)
		{
			return null;
		}
		return fieldNameToFieldMap.get(fieldName);
	}

	public void setFieldNameToFieldMap(Map<String, Field> fieldNameToFieldMap)
	{
		this.fieldNameToFieldMap = fieldNameToFieldMap;
	}

	public void addToFieldNameToFieldMap(String fieldName, Field field)
	{
		this.fieldNameToFieldMap.put(fieldName, field);
	}

	public Map<String, RedisField> getFieldNameToRedisFieldMap()
	{
		return fieldNameToRedisFieldMap;
	}

	public RedisField getRedisFieldFromFieldName(String fieldName)
	{
		if (fieldNameToRedisFieldMap == null)
		{
			return null;
		}
		return fieldNameToRedisFieldMap.get(fieldName);
	}

	public void setFieldNameToRedisFieldMap(Map<String, RedisField> fieldNameToRedisFieldMap)
	{
		this.fieldNameToRedisFieldMap = fieldNameToRedisFieldMap;
	}
}
