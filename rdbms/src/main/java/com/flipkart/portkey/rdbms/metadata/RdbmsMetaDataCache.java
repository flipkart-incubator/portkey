/**
 * 
 */
package com.flipkart.portkey.rdbms.metadata;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.entity.JoinEntity;
import com.flipkart.portkey.common.exception.InvalidAnnotationException;
import com.flipkart.portkey.common.metadata.MetaDataCache;
import com.flipkart.portkey.common.serializer.Serializer;
import com.flipkart.portkey.rdbms.metadata.annotation.Join;
import com.flipkart.portkey.rdbms.metadata.annotation.JoinCriteria;
import com.flipkart.portkey.rdbms.metadata.annotation.JoinField;
import com.flipkart.portkey.rdbms.metadata.annotation.JoinTable;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsDataStore;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;

/**
 * @author santosh.p
 */
public class RdbmsMetaDataCache implements MetaDataCache
{
	private static Logger logger = LoggerFactory.getLogger(RdbmsMetaDataCache.class);
	private Map<Class<? extends Entity>, RdbmsTableMetaData> entityToMetaDataMap;
	private Map<Class<? extends JoinEntity>, RdbmsJoinMetaData> joinEntityToMetaDataMap;
	private static RdbmsMetaDataCache instance = null;

	protected RdbmsMetaDataCache()
	{
		entityToMetaDataMap = new HashMap<Class<? extends Entity>, RdbmsTableMetaData>();
		joinEntityToMetaDataMap = new HashMap<Class<? extends JoinEntity>, RdbmsJoinMetaData>();
	}

	public static RdbmsMetaDataCache getInstance()
	{
		if (instance == null)
		{
			instance = new RdbmsMetaDataCache();
		}
		return instance;
	}

	public <T extends Entity> RdbmsTableMetaData getTableMetaData(Class<T> clazz) throws InvalidAnnotationException
	{
		RdbmsTableMetaData metaData = entityToMetaDataMap.get(clazz);
		if (metaData == null)
		{
			addMetaDataToCache(clazz);
			return entityToMetaDataMap.get(clazz);
		}
		return metaData;
	}

	public <T extends JoinEntity> RdbmsJoinMetaData getJoinMetaData(Class<T> clazz) throws InvalidAnnotationException
	{
		RdbmsJoinMetaData metaData = joinEntityToMetaDataMap.get(clazz);
		if (metaData == null)
		{
			addJoinMetaDataToCache(clazz);
			return joinEntityToMetaDataMap.get(clazz);
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
				if (rdbmsField.defaultInsertValue() != "")
				{
					rdbmsTableMetaData
					        .addToFieldNameToDefaultInsertValueMap(fieldName, rdbmsField.defaultInsertValue());
				}
				if (rdbmsField.defaultUpdateValue() != "")
				{
					rdbmsTableMetaData
					        .addToFieldNameToDefaultUpdateValueMap(fieldName, rdbmsField.defaultUpdateValue());
				}
			}
		}
		if (!primaryKeyPresent)
		{
			throw new InvalidAnnotationException("Primary key is not set for " + clazz);
		}
		String shardKeyField = rdbmsDataStore.shardKeyField();
		rdbmsTableMetaData.setShardKeyFieldName(shardKeyField);
		entityToMetaDataMap.put(clazz, rdbmsTableMetaData);
	}

	private <T extends JoinEntity> void addJoinMetaDataToCache(Class<T> clazz) throws InvalidAnnotationException
	{
		RdbmsJoinMetaData rdbmsJoinMetaData = new RdbmsJoinMetaData();

		Join rdbmsJoin = clazz.getAnnotation(Join.class);
		List<JoinTable> tableList = Arrays.asList(rdbmsJoin.tableList());
		for (JoinTable table : tableList)
		{
			String tableName = table.tableName();
			String alias = table.alias();
			rdbmsJoinMetaData.addToTableList(tableName);
			rdbmsJoinMetaData.addToTableToAliasMap(tableName, alias);
		}
		List<JoinCriteria> joinCriteriaList = Arrays.asList(rdbmsJoin.joinCriteriaList());
		for (JoinCriteria joinCriteria : joinCriteriaList)
		{
			rdbmsJoinMetaData.addToJoinCriteriaList(joinCriteria);
		}
		rdbmsJoinMetaData.setDatabaseName(rdbmsJoin.databaseName());
		rdbmsJoinMetaData.setShardKeyFieldName(rdbmsJoin.shardKeyField());

		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields)
		{
			JoinField joinField = field.getAnnotation(JoinField.class);
			if (joinField != null)
			{
				String tableName = joinField.tableName();
				String columnName = joinField.columnName();
				String fieldName = field.getName();

				rdbmsJoinMetaData.addJoinField(fieldName, joinField);
				rdbmsJoinMetaData.addToFieldNameToFieldMap(fieldName, field);
				rdbmsJoinMetaData.addToFieldNameToTableNameMap(fieldName, tableName);
				rdbmsJoinMetaData.addToFieldNameToColumnNameMap(fieldName, columnName);
				rdbmsJoinMetaData.addToColumnNameToFieldNameMap(columnName, fieldName);

				Serializer serializer;
				try
				{
					serializer = joinField.serializer().newInstance();
				}
				catch (InstantiationException e)
				{
					logger.info("Exception while instantiating serializer for class:" + clazz + ", field:" + joinField);
					throw new InvalidAnnotationException("Exception while fetching serializer for class:" + clazz
					        + ", field:" + joinField + ", exception:" + e.toString());
				}
				catch (IllegalAccessException e)
				{
					logger.info("Exception while instantiating serializer for class:" + clazz + ", field:" + joinField);
					throw new InvalidAnnotationException("Exception while fetching serializer for class:" + clazz
					        + ", field:" + joinField + ", exception:" + e.toString());
				}
				rdbmsJoinMetaData.addToFieldNameToSerializerMap(fieldName, serializer);
			}
		}
		joinEntityToMetaDataMap.put(clazz, rdbmsJoinMetaData);
	}

	public <T extends Entity> String getShardKeyFieldName(Class<T> clazz) throws InvalidAnnotationException
	{
		RdbmsTableMetaData metaData = getTableMetaData(clazz);
		return metaData.getShardKeyFieldName();
	}

	public <T extends JoinEntity> String getJoinShardKeyFieldName(Class<T> clazz) throws InvalidAnnotationException
	{
		RdbmsJoinMetaData metaData = getJoinMetaData(clazz);
		return metaData.getShardKeyFieldName();
	}
}
