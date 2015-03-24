/**
 * 
 */
package com.flipkart.portkey.redis.keyparser;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.InvalidAnnotationException;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.util.PortKeyUtils;
import com.flipkart.portkey.redis.metadata.RedisMetaData;

/**
 * @author santosh.p
 */
public class DefaultKeyParser implements KeyParserInterface
{

	private String parseKeyPattern(String keyPattern, Entity bean, RedisMetaData metaData)
	        throws InvalidAnnotationException
	{
		if (keyPattern == null)
		{
			throw new InvalidAnnotationException("Exception while trying to parse key, key pattern is null, bean:"
			        + bean);
		}
		Matcher fieldNameMatcher = Pattern.compile("\\{(.*?)\\}").matcher(keyPattern);
		List<String> fieldNames = new ArrayList<String>();
		while (fieldNameMatcher.find())
		{
			fieldNames.add(fieldNameMatcher.group(1));
		}
		for (String fieldName : fieldNames)
		{
			Field field = metaData.getFieldFromFieldName(fieldName);
			if (field == null)
			{
				throw new InvalidAnnotationException("Exception while trying to parse key, key field is null, bean:"
				        + bean);
			}
			Object value;
			try
			{
				if (!field.isAccessible())
				{
					field.setAccessible(true);
				}
				value = field.get(bean);
			}
			catch (IllegalArgumentException e)
			{
				throw new InvalidAnnotationException("Exception while trying to fetch key value from bean, bean:"
				        + bean + "\nexception:" + e);
			}
			catch (IllegalAccessException e)
			{
				throw new InvalidAnnotationException("Exception while trying to fetch key value from bean, bean:"
				        + bean + "\nexception:" + e);
			}
			catch (NullPointerException e)
			{
				throw new InvalidAnnotationException(
				        "Exception while trying to fetch key from bean, passed bean is null.");
			}
			String fieldVal;
			if (value == null)
			{
				fieldVal = null;
			}
			else if (value.getClass().isEnum())
			{
				fieldVal = value.toString();
			}
			else
			{
				fieldVal = PortKeyUtils.toString(value);
			}
			keyPattern = keyPattern.replace("{" + fieldName + "}", fieldVal);
		}
		return keyPattern;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.redis.keyparser.KeyParserInterface#parseKey(java.lang.String,
	 * com.flipkart.portkey.common.entity.Entity, com.flipkart.portkey.redis.metadata.RedisMetaData)
	 */
	public List<String> parsePrimaryKeyPattern(Entity bean, RedisMetaData metaData) throws InvalidAnnotationException
	{
		String keyPattern = metaData.getPrimaryKeyPattern();
		String key = parseKeyPattern(keyPattern, bean, metaData);
		if (key == null)
		{
			return null;
		}
		return Arrays.asList(key.split("->"));
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.flipkart.portkey.redis.keyparser.KeyParserInterface#parseSecondaryKeyPatterns(com.flipkart.portkey.common
	 * .entity.Entity, com.flipkart.portkey.redis.metadata.RedisMetaData)
	 */
	public List<String> parseSecondaryKeyPatterns(Entity bean, RedisMetaData metaData)
	        throws InvalidAnnotationException
	{
		List<String> keyPatterns = metaData.getSecondaryKeyPatterns();
		List<String> parsedKeyPatterns = new ArrayList<String>();
		for (String keyPattern : keyPatterns)
		{
			String parsedKeyPattern = parseKeyPattern(keyPattern, bean, metaData);
			parsedKeyPatterns.add(parsedKeyPattern);
		}
		return parsedKeyPatterns;
	}

	/**
	 * @param keyPattern
	 * @param attributeToValueMap
	 * @param metaData
	 * @return
	 * @throws PortKeyException
	 */
	private String parseKeyPattern(String keyPattern, Map<String, Object> attributeToValueMap, RedisMetaData metaData)
	        throws InvalidAnnotationException
	{
		if (keyPattern == null)
		{
			throw new InvalidAnnotationException(
			        "Exception while trying to parse primary key, primary key pattern is null");
		}
		Matcher attributeMatcher = Pattern.compile("\\{(.*?)\\}").matcher(keyPattern);
		List<String> attributes = new ArrayList<String>();
		while (attributeMatcher.find())
		{
			attributes.add(attributeMatcher.group(1));
		}
		for (String attribute : attributes)
		{
			Object value = attributeToValueMap.get(attribute);
			String fieldVal;
			if (value == null)
			{
				fieldVal = null;
			}
			else if (value.getClass().isEnum())
			{
				fieldVal = value.toString();
			}
			else
			{
				fieldVal = PortKeyUtils.toString(value);
			}
			keyPattern = keyPattern.replace("{" + attribute + "}", fieldVal);
		}
		return keyPattern;
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.redis.keyparser.KeyParserInterface#parsePrimaryKeyPattern(java.util.Map,
	 * com.flipkart.portkey.redis.metadata.RedisMetaData)
	 */
	public List<String> parsePrimaryKeyPattern(Map<String, Object> attributeToValueMap, RedisMetaData metaData)
	        throws InvalidAnnotationException
	{
		String keyPattern = metaData.getPrimaryKeyPattern();
		String key = parseKeyPattern(keyPattern, attributeToValueMap, metaData);
		if (key == null)
		{
			return null;
		}
		return Arrays.asList(key.split("->"));
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.redis.keyparser.KeyParserInterface#parseSecondaryKeyPattern(java.util.Map,
	 * com.flipkart.portkey.redis.metadata.RedisMetaData)
	 */
	public String parseSecondaryKeyPattern(Map<String, Object> attributeToValueMap, RedisMetaData metaData)
	        throws InvalidAnnotationException
	{
		List<String> keyPatterns = metaData.getSecondaryKeyPatterns();
		for (String keyPattern : keyPatterns)
		{
			List<String> attributeList = metaData.getAttributeListFromKeyPattern(keyPattern);
			Set<String> attributeSet = new HashSet<String>(attributeList);
			if (attributeSet.equals(attributeToValueMap.keySet()))
			{
				return parseKeyPattern(keyPattern, attributeToValueMap, metaData);
			}
		}
		return null;
	}
}
