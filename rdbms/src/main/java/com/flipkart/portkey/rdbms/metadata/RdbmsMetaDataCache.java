/**
 * 
 */
package com.flipkart.portkey.rdbms.metadata;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.InvalidAnnotationException;
import com.flipkart.portkey.common.metadata.MetaDataCache;
import com.flipkart.portkey.common.serializer.Serializer;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsDataStore;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;

/**
 * @author santosh.p
 */
public class RdbmsMetaDataCache implements MetaDataCache
{
	private static Logger logger = Logger.getLogger(RdbmsMetaDataCache.class);
	private Map<Class<? extends Entity>, RdbmsTableMetaData> entityToMetaDataMap;
	private static RdbmsMetaDataCache instance = null;

	protected RdbmsMetaDataCache()
	{
		entityToMetaDataMap = new HashMap<Class<? extends Entity>, RdbmsTableMetaData>();
	}

	public static RdbmsMetaDataCache getInstance()
	{
		if (instance == null)
		{
			instance = new RdbmsMetaDataCache();
		}
		return instance;
	}

	public <T extends Entity> RdbmsTableMetaData getMetaData(Class<T> clazz) throws InvalidAnnotationException
	{
		RdbmsTableMetaData metaData = entityToMetaDataMap.get(clazz);
		if (metaData == null)
		{
			addMetaDataToCache(clazz);
			return entityToMetaDataMap.get(clazz);
		}
		return metaData;
	}

	private <T extends Entity> void addMetaDataToCache(Class<T> clazz) throws InvalidAnnotationException
	{
		RdbmsTableMetaData rdbmsTableMetaData = new RdbmsTableMetaData();

		RdbmsDataStore rdbmsDataStore = clazz.getAnnotation(RdbmsDataStore.class);
		rdbmsTableMetaData.setDatabaseName(rdbmsDataStore.databaseName());
		rdbmsTableMetaData.setTableName(rdbmsDataStore.tableName());

		Field[] fields = clazz.getDeclaredFields();
		boolean primaryKeyPresent = false;
		for (Field field : fields)
		{
			RdbmsField rdbmsField = field.getAnnotation(RdbmsField.class);
			if (rdbmsField != null)
			{
				String columnName = rdbmsField.columnName();
				String fieldName = field.getName();
				rdbmsTableMetaData.addToFieldNameToRdbmsFieldMap(fieldName, rdbmsField);
				rdbmsTableMetaData.addToFieldNameToColumnNameMap(fieldName, columnName);
				rdbmsTableMetaData.addToColumnNameToFieldNameMap(columnName, fieldName);
				rdbmsTableMetaData.addToFieldNameToFieldMap(fieldName, field);
				Serializer serializer;
				try
				{
					serializer = rdbmsField.serializer().newInstance();
				}
				catch (InstantiationException e)
				{
					logger.warn("Exception while fetching serializer for class:" + clazz + ", field:" + field
					        + ", exception:" + e);
					throw new InvalidAnnotationException("Exception while initializing serializer for class:" + clazz
					        + ", field:" + field, e);
				}
				catch (IllegalAccessException e)
				{
					logger.warn("Exception while fetching serializer for class:" + clazz + ", field:" + field
					        + ", exception:" + e);
					throw new InvalidAnnotationException("Exception while initializing serializer for class:" + clazz
					        + ", field:" + field, e);
				}
				rdbmsTableMetaData.addToFieldNameToSerializerMap(fieldName, serializer);
				if (rdbmsField.isPrimaryKey())
				{
					primaryKeyPresent = true;
					rdbmsTableMetaData.addPrimaryKey(fieldName);
				}
				if (rdbmsField.defaultValue() != "")
				{
					rdbmsTableMetaData.addToFieldNameToDefaultValueMap(fieldName, rdbmsField.defaultValue());
				}
			}
		}
		if (!primaryKeyPresent)
		{
			throw new InvalidAnnotationException("Primary key is not set for class" + clazz);
		}
		String shardKeyField = rdbmsDataStore.shardKeyField();
		rdbmsTableMetaData.setShardKeyFieldName(shardKeyField);
		entityToMetaDataMap.put(clazz, rdbmsTableMetaData);
	}

	public <T extends Entity> String getShardKeyFieldName(Class<T> clazz) throws InvalidAnnotationException
	{
		RdbmsTableMetaData rdbmsTableMetaData = getMetaData(clazz);
		return rdbmsTableMetaData.getShardKeyFieldName();
	}
}
