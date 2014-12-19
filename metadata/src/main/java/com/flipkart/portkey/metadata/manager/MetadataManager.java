/**
 * 
 */
package com.flipkart.portkey.metadata.manager;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.metadata.annotation.RdbmsField;
import com.flipkart.portkey.metadata.annotation.RdbmsTable;
import com.flipkart.portkey.metadata.annotation.RedisField;
import com.flipkart.portkey.metadata.annotation.RedisTable;
import com.flipkart.portkey.metadata.datastore.RdbmsTableMetaData;
import com.flipkart.portkey.metadata.datastore.RedisTableMetaData;

/**
 * @author santosh.p
 */
public class MetadataManager
{
	private Map<Class<? extends Entity>, Map<DataStoreType, RdbmsTableMetaData>> rdbmsTableMetaDataMap;
	private Map<Class<? extends Entity>, Map<DataStoreType, RedisTableMetaData>> redisTableMetaDataMap;

	public RdbmsTableMetaData getRdbmsMetaData(Class<? extends Entity> clazz, DataStoreType dataStore, Entity bean)
	{
		Map<DataStoreType, RdbmsTableMetaData> metaData = rdbmsTableMetaDataMap.get(clazz);
		if (metaData == null)
		{
			initializeMetaDataMapsForClass(clazz);
		}
		RdbmsTableMetaData rdbmsMetaData = metaData.get(dataStore);
		if (rdbmsMetaData == null)
		{
			addEntityMetaData(clazz, bean);
			return metaData.get(dataStore);
		}
		return rdbmsMetaData;
	}

	public RedisTableMetaData getRedisMetaData(Class<? extends Entity> clazz, DataStoreType dataStore, Entity bean)
	{
		Map<DataStoreType, RedisTableMetaData> metaData = redisTableMetaDataMap.get(clazz);
		if (metaData == null)
		{
			initializeMetaDataMapsForClass(clazz);
		}
		RedisTableMetaData redisMetaData = metaData.get(dataStore);
		if (redisMetaData == null)
		{
			addEntityMetaData(clazz, bean);
			return metaData.get(dataStore);
		}
		return redisMetaData;
	}

	private void initializeMetaDataMapsForClass(Class<? extends Entity> clazz)
	{
		Map<DataStoreType, RdbmsTableMetaData> rdbmsMetaDataMap = new HashMap<DataStoreType, RdbmsTableMetaData>();
		rdbmsTableMetaDataMap.put(clazz, rdbmsMetaDataMap);

		Map<DataStoreType, RedisTableMetaData> redisMetaDataMap = new HashMap<DataStoreType, RedisTableMetaData>();
		redisTableMetaDataMap.put(clazz, redisMetaDataMap);
	}

	/**
	 * @param clazz
	 * @param bean
	 */
	private void addEntityMetaData(Class<? extends Entity> clazz, Entity bean)
	{
		// metadata specific to rdbms
		RdbmsTableMetaData rdbmsTableMetaData = new RdbmsTableMetaData();
		// metadata specific to redis
		RedisTableMetaData redisTableMetaData = new RedisTableMetaData();

		RdbmsTable rdbmsTable = clazz.getAnnotation(RdbmsTable.class);
		rdbmsTableMetaData.setTableName(rdbmsTable.tableName());
		rdbmsTableMetaData.setDatabaseName(rdbmsTable.databaseName());

		RedisTable redisTable = clazz.getAnnotation(RedisTable.class);
		redisTableMetaData.setDatabase(redisTable.database());
		redisTableMetaData.setPrimaryKey(redisTable.primaryKey());
		redisTableMetaData.setSecondaryKeys(Arrays.asList(redisTable.secondaryKeys()));

		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields)
		{
			RdbmsField rdbmsField = field.getAnnotation(RdbmsField.class);
			if (rdbmsField != null)
			{
				String columnName = rdbmsField.columnName();
				String fieldName = field.getName();
				rdbmsTableMetaData.addToFieldNameToRdbmsColumnMap(fieldName, columnName);
				rdbmsTableMetaData.addToRdbmsColumnToFieldNameMap(columnName, fieldName);
				rdbmsTableMetaData.addToRdbmsFieldList(rdbmsField);
				if (rdbmsField.isPrimaryKey())
				{
					rdbmsTableMetaData.addToPrimaryKeys(columnName);
				}
				if (rdbmsField.isJson())
				{
					rdbmsTableMetaData.addToJsonFields(columnName);
				}
				if (rdbmsField.isJsonList())
				{
					rdbmsTableMetaData.addToJsonListFields(columnName);
				}
			}
			RedisField redisField = field.getAnnotation(RedisField.class);
			if (redisField != null)
			{
				String attributeName = redisField.attributeName();
				String fieldName = field.getName();
				redisTableMetaData.addToFieldNameToAttributeNameMap(fieldName, attributeName);
				redisTableMetaData.addToAttributeNameToFieldNameMap(attributeName, fieldName);

				if (redisField.isJson())
				{
					redisTableMetaData.addToJsonFields(attributeName);
				}
				if (redisField.isJsonList())
				{
					redisTableMetaData.addToJsonListFields(attributeName);
				}
			}
		}
		rdbmsTableMetaDataMap.get(clazz).put(DataStoreType.RDBMS, rdbmsTableMetaData);
		redisTableMetaDataMap.get(clazz).put(DataStoreType.REDIS, redisTableMetaData);
	}
}
