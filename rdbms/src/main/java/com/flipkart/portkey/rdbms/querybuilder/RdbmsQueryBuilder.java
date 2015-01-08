/**
 * 
 */
package com.flipkart.portkey.rdbms.querybuilder;

import java.lang.reflect.Field;
import java.util.List;

import org.apache.ibatis.jdbc.SqlBuilder;
import org.apache.log4j.Logger;

import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;

/**
 * @author santosh.p
 */
public class RdbmsQueryBuilder
{
	private static Logger logger = Logger.getLogger(RdbmsQueryBuilder.class);
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
					valuesQryStrBuilder.append(":" + rdbmsField.columnName() + ",");
				}

			}
			SqlBuilder.INSERT_INTO(tableMetaData.getTableName());

			SqlBuilder.VALUES(insertQryStrBuilder.substring(0, insertQryStrBuilder.length() - 1),
			        valuesQryStrBuilder.substring(0, valuesQryStrBuilder.length() - 1));
			insertQuery = SqlBuilder.SQL();
			tableMetaData.setInsertQuery(insertQuery);
		}
		logger.debug(insertQuery);
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
						SqlBuilder.SET(rdbmsField.columnName() + "=:" + rdbmsField.columnName());
					}
					else
					{
						SqlBuilder.WHERE(rdbmsField.columnName() + "=:" + rdbmsField.columnName());
					}
				}
			}

			updateQuery = SqlBuilder.SQL();
			tableMetaData.setUpdateByPkQuery(updateQuery);
		}
		logger.debug(updateQuery);
		return updateQuery;
	}

	public String getUpdateByCriteriaQuery(String tableName, List<String> updateAttributes,
	        List<String> criteriaAttributes)
	{
		SqlBuilder.BEGIN();
		SqlBuilder.UPDATE(tableName);
		for (String attribute : updateAttributes)
		{
			SqlBuilder.SET(attribute + "=:" + attribute);
		}
		for (String criteria : criteriaAttributes)
		{

			SqlBuilder.WHERE(criteria + "=:" + criteria);
		}

		String updateQuery = SqlBuilder.SQL();
		logger.debug(updateQuery);
		return updateQuery;
	}

	public String getDeleteByCriteriaQuery(String tableName, List<String> criteriaAttributes)
	{
		SqlBuilder.BEGIN();
		SqlBuilder.DELETE_FROM(tableName);
		for (String attribute : criteriaAttributes)
		{
			SqlBuilder.WHERE(attribute + "=:" + attribute);
		}

		String updateQuery = SqlBuilder.SQL();
		logger.debug(updateQuery);
		return updateQuery;
	}

	/**
	 * @param tableMetaData
	 * @param criteria
	 * @return
	 */
	public String getGetByCriteriaQuery(String tableName, List<String> criteriaAttributes)
	{
		SqlBuilder.BEGIN();
		SqlBuilder.SELECT("*");
		SqlBuilder.FROM(tableName);
		for (String attribute : criteriaAttributes)
		{
			SqlBuilder.WHERE(attribute + "=:" + attribute);
		}
		String getQuery = SqlBuilder.SQL();
		logger.debug(getQuery);
		return getQuery;
	}

	/**
	 * @param tableMetaData
	 * @param selectAttributes
	 * @param criteria
	 * @return
	 */
	public String getGetByCriteriaQuery(String tableName, List<String> selectAttributes, List<String> criteriaAttributes)
	{
		SqlBuilder.BEGIN();
		for (String attribute : selectAttributes)
		{
			SqlBuilder.SELECT(attribute);
		}
		SqlBuilder.FROM(tableName);
		for (String attribute : criteriaAttributes)
		{
			SqlBuilder.WHERE(attribute + "=:" + attribute);
		}
		String getQuery = SqlBuilder.SQL();
		logger.debug(getQuery);
		return getQuery;
	}
}
