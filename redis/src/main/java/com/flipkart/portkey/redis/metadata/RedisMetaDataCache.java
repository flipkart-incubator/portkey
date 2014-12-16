/**
 * 
 */
package com.flipkart.portkey.redis.metadata;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.redis.metadata.annotation.RedisDataStore;
import com.flipkart.portkey.redis.metadata.annotation.RedisField;

/**
 * @author santosh.p
 */
public class RedisMetaDataCache
{
	private static Map<Class<? extends Entity>, RedisMetaData> entityToMetaDataMap;

	public static RedisMetaData getMetaData(Class<? extends Entity> clazz) throws PortKeyException
	{
		RedisMetaData metaData = entityToMetaDataMap.get(clazz);
		if (metaData == null)
		{
			addMetaDataToCache(clazz);
			return entityToMetaDataMap.get(clazz);
		}
		return metaData;
	}

	/**
	 * @param keyword
	 * @param bean
	 * @param metaData
	 * @return
	 * @throws PortKeyException
	 */
	private static String getReplacementValueForKeyWord(String keyword, Class<? extends Entity> clazz)
	        throws PortKeyException
	{
		if (keyword.equalsIgnoreCase("CLASS"))
		{
			return clazz.toString();
		}
		throw new PortKeyException("Unrecognized keyword");
	}

	private static String parseKeyPattern(String keyPattern, Class<? extends Entity> clazz) throws PortKeyException
	{
		String key;
		key = keyPattern;
		Matcher keywordMatcher = Pattern.compile("\\[(.*?)\\]").matcher(key);
		List<String> keywords = new ArrayList<String>();
		while (keywordMatcher.find())
		{
			keywords.add(keywordMatcher.group(1));
		}
		for (String keyword : keywords)
		{
			String replacementValue = getReplacementValueForKeyWord(keyword, clazz);
			key = key.replace("[" + keyword + "]", replacementValue);
		}
		return key;
	}

	/**
	 * @param clazz
	 * @param bean
	 * @throws PortKeyException
	 */
	private static void addMetaDataToCache(Class<? extends Entity> clazz) throws PortKeyException
	{
		// metadata specific to redis
		RedisMetaData redisMetaData = new RedisMetaData();

		RedisDataStore redisDataStore = clazz.getAnnotation(RedisDataStore.class);
		redisMetaData.setDatabase(redisDataStore.database());
		String primaryKeyPattern = redisDataStore.primaryKeyPattern();
		String parsedPrimaryKeyPattern = parseKeyPattern(primaryKeyPattern, clazz);
		redisMetaData.setPrimaryKeyPattern(parsedPrimaryKeyPattern);
		List<String> secondaryKeyPatterns = Arrays.asList(redisDataStore.secondaryKeyPatterns());
		for (String secondaryKeyPattern : secondaryKeyPatterns)
		{
			String parsedPattern = parseKeyPattern(secondaryKeyPattern, clazz);
			redisMetaData.addToSecondaryKeyPatterns(parsedPattern);
		}
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields)
		{
			RedisField redisField = field.getAnnotation(RedisField.class);
			if (redisField != null)
			{
				String attributeName = redisField.attributeName();
				String fieldName = field.getName();
				redisMetaData.addToFieldNameToAttributeMap(fieldName, attributeName);
				redisMetaData.addToAttributeToFieldNameMap(attributeName, fieldName);
				redisMetaData.addToFieldNameToFieldMap(fieldName, field);
				redisMetaData.addToRedisFieldList(redisField);
				if (redisField.isJson())
				{
					redisMetaData.addToJsonFields(attributeName);
				}
				if (redisField.isJsonList())
				{
					redisMetaData.addToJsonListFields(attributeName);
				}
			}
		}
		entityToMetaDataMap.put(clazz, redisMetaData);
	}
}
