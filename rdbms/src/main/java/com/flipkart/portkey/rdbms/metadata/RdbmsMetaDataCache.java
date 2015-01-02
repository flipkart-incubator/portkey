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
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsTable;

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

	/**
	 * @param clazz
	 * @param bean
	 * @throws InvalidAnnotationException
	 */
	private <T extends Entity> void addMetaDataToCache(Class<T> clazz) throws InvalidAnnotationException
	{
		// metadata specific to rdbms
		RdbmsTableMetaData rdbmsTableMetaData = new RdbmsTableMetaData();

		RdbmsTable rdbmsTable = clazz.getAnnotation(RdbmsTable.class);
		rdbmsTableMetaData.setTableName(rdbmsTable.tableName());
		rdbmsTableMetaData.setDatabaseName(rdbmsTable.databaseName());

		Field[] fields = clazz.getDeclaredFields();
		boolean shardKeyPresent = false;
		boolean primaryKeyPresent = false;
		for (Field field : fields)
		{
			RdbmsField rdbmsField = field.getAnnotation(RdbmsField.class);
			if (rdbmsField != null)
			{
				String columnName = rdbmsField.columnName();
				String fieldName = field.getName();
				rdbmsTableMetaData.addRdbmsField(fieldName, rdbmsField);
				rdbmsTableMetaData.addToFieldNameToRdbmsColumnMap(fieldName, columnName);
				rdbmsTableMetaData.addToRdbmsColumnToFieldNameMap(columnName, fieldName);
				rdbmsTableMetaData.addToFieldNameToFieldMap(fieldName, field);
				Serializer serializer;
				try
				{
					serializer = rdbmsField.serializer().newInstance();
				}
				catch (InstantiationException e)
				{
					logger.info("Exception while fetching serializer for class:" + clazz);
					throw new InvalidAnnotationException("Exception while fetching serializer for class:" + clazz, e);
				}
				catch (IllegalAccessException e)
				{
					logger.info("Exception while fetching serializer for class:" + clazz);
					throw new InvalidAnnotationException("Exception while fetching serializer for class:" + clazz, e);
				}
				rdbmsTableMetaData.setSerializer(fieldName, serializer);
				if (rdbmsField.isPrimaryKey())
				{
					primaryKeyPresent = true;
					rdbmsTableMetaData.addToPrimaryKeys(columnName);
				}
				if (rdbmsField.isShardKey())
				{
					shardKeyPresent = true;
					rdbmsTableMetaData.setShardKey(columnName);
				}

			}
		}
		if (!primaryKeyPresent)
		{
			throw new InvalidAnnotationException("Primary key is not set for class" + clazz);
		}
		if (!shardKeyPresent)
		{
			throw new InvalidAnnotationException("Shard key is not set for class" + clazz);
		}
		entityToMetaDataMap.put(clazz, rdbmsTableMetaData);
	}

	/*
	 * (non-Javadoc)
	 * @see com.flipkart.portkey.common.metadata.MetaDataCache#getShardKey(java.lang.Class)
	 */
	public <T extends Entity> String getShardKey(Class<T> clazz) throws InvalidAnnotationException
	{
		RdbmsTableMetaData tableMetaData = getMetaData(clazz);
		return tableMetaData.getShardKey();
	}
}
