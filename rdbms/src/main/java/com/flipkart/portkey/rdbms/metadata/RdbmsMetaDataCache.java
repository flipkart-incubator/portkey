/**
 * 
 */
package com.flipkart.portkey.rdbms.metadata;

import java.lang.reflect.Field;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsTable;

/**
 * @author santosh.p
 */
public class RdbmsMetaDataCache
{
	private static Map<Class<? extends Entity>, RdbmsTableMetaData> entityToMetaDataMap;

	public static RdbmsTableMetaData getMetaData(Class<? extends Entity> clazz)
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
	 */
	private static void addMetaDataToCache(Class<? extends Entity> clazz)
	{
		// metadata specific to rdbms
		RdbmsTableMetaData rdbmsTableMetaData = new RdbmsTableMetaData();

		RdbmsTable rdbmsTable = clazz.getAnnotation(RdbmsTable.class);
		rdbmsTableMetaData.setTableName(rdbmsTable.tableName());
		rdbmsTableMetaData.setDatabaseName(rdbmsTable.databaseName());

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
				rdbmsTableMetaData.addToFieldNameToFieldMap(fieldName, field);
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
		}
		entityToMetaDataMap.put(clazz, rdbmsTableMetaData);
	}
}
