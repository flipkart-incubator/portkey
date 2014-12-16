/**
 * 
 */
package com.flipkart.portkey.rdbms.querybuilder;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.jdbc.SqlBuilder;

import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;

/**
 * @author santosh.p
 */
public class RdbmsQueryBuilder
{
	private static RdbmsQueryBuilder instance;

	public static RdbmsQueryBuilder getInstance()
	{
		if (instance == null)
		{
			instance = new RdbmsQueryBuilder();
		}
		return instance;
	}

	public String getInsertQuery(RdbmsTableMetaData tableMetaData)
	{
		String insertQuery = tableMetaData.getInsertQuery();
		if (insertQuery == null || insertQuery.isEmpty())
		{
			SqlBuilder.BEGIN();
			List<Field> fieldList = tableMetaData.getFieldList();

			StringBuilder insertQryStrBuilder = new StringBuilder();
			StringBuilder valuesQryStrBuilder = new StringBuilder();

			for (Field field : fieldList)
			{
				RdbmsField rdbmsField = field.getAnnotation(RdbmsField.class);

				if (rdbmsField != null)
				{
					insertQryStrBuilder.append(rdbmsField.columnName() + ",");
					valuesQryStrBuilder.append(":" + field.getName() + ",");
				}

			}
			SqlBuilder.INSERT_INTO(tableMetaData.getTableName());

			SqlBuilder.VALUES(insertQryStrBuilder.substring(0, insertQryStrBuilder.length() - 1),
			        valuesQryStrBuilder.substring(0, valuesQryStrBuilder.length() - 1));
			insertQuery = SqlBuilder.SQL();
			tableMetaData.setInsertQuery(insertQuery);
		}
		return insertQuery;
	}

	/**
	 * @param metaData
	 * @return
	 */
	public String getUpdateByPkQuery(RdbmsTableMetaData tableMetaData)
	{
		String updateQuery = tableMetaData.getUpdateByPkQuery();
		if (updateQuery == null || updateQuery.isEmpty())
		{
			SqlBuilder.BEGIN();
			SqlBuilder.UPDATE(tableMetaData.getTableName());

			List<Field> fieldList = tableMetaData.getFieldList();

			for (Field field : fieldList)
			{
				RdbmsField rdbmsField = field.getAnnotation(RdbmsField.class);
				if (rdbmsField != null)
				{
					if (!rdbmsField.isPrimaryKey())
					{
						SqlBuilder.SET(rdbmsField.columnName() + "=:" + field.getName());
					}
					else
					{
						SqlBuilder.WHERE(rdbmsField.columnName() + "=:" + field.getName());
					}
				}
			}

			updateQuery = SqlBuilder.SQL();
			tableMetaData.setUpdateByPkQuery(updateQuery);
		}
		return updateQuery;
	}

	/**
	 * @param updateAttributesToValuesMap
	 * @param criteria
	 * @return
	 */
	public String getUpdateByCriteriaQuery(RdbmsTableMetaData tableMetaData,
	        Map<String, Object> updateAttributesToValuesMap, Map<String, Object> criteriaMap)
	{
		SqlBuilder.BEGIN();
		SqlBuilder.UPDATE(tableMetaData.getTableName());
		for (String attribute : updateAttributesToValuesMap.keySet())
		{
			SqlBuilder.SET(attribute + "=" + updateAttributesToValuesMap.get(attribute));
		}
		for (String criteria : criteriaMap.keySet())
		{

			SqlBuilder.WHERE(criteria + "=" + criteriaMap.get(criteria));
		}

		String updateQuery = SqlBuilder.SQL();
		return updateQuery;
	}

	/**
	 * @param tableMetaData
	 * @param criteria
	 * @return
	 */
	public String getDeleteByCriteriaQuery(RdbmsTableMetaData tableMetaData, Map<String, Object> criteria)
	{
		SqlBuilder.BEGIN();
		SqlBuilder.DELETE_FROM(tableMetaData.getTableName());
		for (String attribute : criteria.keySet())
		{
			SqlBuilder.WHERE(attribute + "=" + criteria.get(attribute));
		}

		String updateQuery = SqlBuilder.SQL();
		return updateQuery;
	}

	/**
	 * @param tableMetaData
	 * @param criteria
	 * @return
	 */
	public String getGetByCriteriaQuery(RdbmsTableMetaData tableMetaData, Map<String, Object> criteria)
	{
		SqlBuilder.BEGIN();
		SqlBuilder.SELECT("*");
		SqlBuilder.FROM(tableMetaData.getTableName());
		for (String attribute : criteria.keySet())
		{
			SqlBuilder.WHERE(attribute + "=" + criteria.get(attribute));
		}
		String getQuery = SqlBuilder.SQL();
		return getQuery;
	}

	/**
	 * @param tableMetaData
	 * @param attributeNames
	 * @param criteria
	 * @return
	 */
	public String getGetByCriteriaQuery(RdbmsTableMetaData tableMetaData, List<String> attributeNames,
	        Map<String, Object> criteria)
	{
		SqlBuilder.BEGIN();
		for (String attributeName : attributeNames)
		{
			SqlBuilder.SELECT(attributeName);
		}
		SqlBuilder.FROM(tableMetaData.getTableName());
		for (String attribute : criteria.keySet())
		{
			SqlBuilder.WHERE(attribute + "=" + criteria.get(attribute));
		}
		String getQuery = SqlBuilder.SQL();
		return getQuery;
	}
}
