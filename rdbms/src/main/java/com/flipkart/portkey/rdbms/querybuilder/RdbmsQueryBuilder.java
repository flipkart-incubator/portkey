/**
 * 
 */
package com.flipkart.portkey.rdbms.querybuilder;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.jdbc.SqlBuilder;
import org.apache.log4j.Logger;

import com.flipkart.portkey.common.util.PortKeyUtils;
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
			List<Field> fieldsList = tableMetaData.getFieldsList();

			StringBuilder insertQueryStrBuilder = new StringBuilder();
			StringBuilder valuesQryStrBuilder = new StringBuilder();

			for (Field field : fieldsList)
			{
				RdbmsField rdbmsField = field.getAnnotation(RdbmsField.class);

				if (rdbmsField != null)
				{
					insertQueryStrBuilder.append("`" + rdbmsField.columnName() + "`" + ",");
					if (rdbmsField.defaultInsertValue().equals(""))
					{
						valuesQryStrBuilder.append(":" + rdbmsField.columnName() + ",");
					}
					else
					{
						valuesQryStrBuilder.append(rdbmsField.defaultInsertValue() + ",");
					}
				}

			}
			SqlBuilder.INSERT_INTO(tableMetaData.getTableName());

			SqlBuilder.VALUES(insertQueryStrBuilder.substring(0, insertQueryStrBuilder.length() - 1),
			        valuesQryStrBuilder.substring(0, valuesQryStrBuilder.length() - 1));
			insertQuery = SqlBuilder.SQL();
			tableMetaData.setInsertQuery(insertQuery);
		}
		logger.debug(insertQuery);
		return insertQuery;
	}

	public String getUpsertQuery(RdbmsTableMetaData tableMetaData)
	{
		String upsertQuery = tableMetaData.getUpsertQuery();
		if (upsertQuery == null || upsertQuery.isEmpty())
		{
			String insertQuery = getInsertQuery(tableMetaData);
			StringBuilder onDuplicateQueryStrBuilder = new StringBuilder();
			onDuplicateQueryStrBuilder.append("\nON DUPLICATE KEY UPDATE ");
			List<String> primaryKeys = tableMetaData.getPrimaryKeysList();
			for (String fieldName : tableMetaData.getFieldNameToColumnNameMap().keySet())
			{
				if (primaryKeys.contains(fieldName))
				{
					continue;
				}
				String columnName = tableMetaData.getColumnNameFromFieldName(fieldName);
				RdbmsField rdbmsField = tableMetaData.getRdbmsFieldFromFieldName(fieldName);
				if (rdbmsField.defaultUpdateValue().equals(""))
				{
					onDuplicateQueryStrBuilder.append("`" + columnName + "`" + "=:" + columnName + ",");
				}
				else
				{
					onDuplicateQueryStrBuilder.append("`" + columnName + "`" + "=" + rdbmsField.defaultUpdateValue()
					        + ",");
				}
			}
			upsertQuery =
			        insertQuery + onDuplicateQueryStrBuilder.substring(0, onDuplicateQueryStrBuilder.length() - 1);
		}
		logger.debug(upsertQuery);
		return upsertQuery;
	}

	public String getUpsertQuery(RdbmsTableMetaData tableMetaData, List<String> fieldsToBeUpdatedOnDuplicate)
	{
		String insertQuery = getInsertQuery(tableMetaData);
		StringBuilder onDuplicateQueryStrBuilder = new StringBuilder();
		onDuplicateQueryStrBuilder.append("\nON DUPLICATE KEY UPDATE ");
		for (String fieldName : fieldsToBeUpdatedOnDuplicate)
		{
			String columnName = tableMetaData.getColumnNameFromFieldName(fieldName);
			RdbmsField rdbmsField = tableMetaData.getRdbmsFieldFromFieldName(fieldName);
			if (rdbmsField.defaultUpdateValue().equals(""))
			{
				onDuplicateQueryStrBuilder.append("`" + columnName + "`" + "=:" + columnName + ",");
			}
			else
			{
				onDuplicateQueryStrBuilder.append("`" + columnName + "`" + "=" + rdbmsField.defaultUpdateValue() + ",");
			}
		}
		String upsertQuery =
		        insertQuery + onDuplicateQueryStrBuilder.substring(0, onDuplicateQueryStrBuilder.length() - 1);
		logger.debug(upsertQuery);
		return upsertQuery;
	}

	public String getUpdateByPkQuery(RdbmsTableMetaData tableMetaData)
	{
		String updateQuery = tableMetaData.getUpdateByPkQuery();
		if (updateQuery == null || updateQuery.isEmpty())
		{
			SqlBuilder.BEGIN();
			SqlBuilder.UPDATE(tableMetaData.getTableName());

			List<Field> fieldsList = tableMetaData.getFieldsList();

			for (Field field : fieldsList)
			{
				RdbmsField rdbmsField = field.getAnnotation(RdbmsField.class);
				if (rdbmsField != null)
				{
					if (!rdbmsField.isPrimaryKey())
					{
						if (rdbmsField.defaultUpdateValue().equals(""))
						{
							SqlBuilder.SET("`" + rdbmsField.columnName() + "`" + "=:" + rdbmsField.columnName());
						}
						else
						{
							SqlBuilder.SET("`" + rdbmsField.columnName() + "`" + "=" + rdbmsField.defaultUpdateValue()
							        + ",");
						}
					}
					else
					{
						SqlBuilder.WHERE("`" + rdbmsField.columnName() + "`" + "=:" + rdbmsField.columnName());
					}
				}
			}

			updateQuery = SqlBuilder.SQL();
			tableMetaData.setUpdateByPkQuery(updateQuery);
		}
		logger.debug(updateQuery);
		return updateQuery;
	}

	public String getUpdateByCriteriaQuery(String tableName, List<String> columnsToBeUpdated,
	        List<String> columnsInCriteria, Map<String, Object> columnToValueMap)
	{
		SqlBuilder.BEGIN();
		SqlBuilder.UPDATE(tableName);
		for (String column : columnsToBeUpdated)
		{
			if (columnToValueMap.get(column) != null
			        && columnToValueMap.get(column).getClass().equals(RdbmsSpecialValue.class))
			{
				RdbmsSpecialValue specialValue = (RdbmsSpecialValue) columnToValueMap.get(column);
				SqlBuilder.SET("`" + column + "`" + "=" + PortKeyUtils.toString(specialValue));
			}
			else
			{
				SqlBuilder.SET("`" + column + "`" + "=:" + column);
			}
		}
		for (String column : columnsInCriteria)
		{
			if (columnToValueMap.get(column) != null)
			{
				SqlBuilder.WHERE("`" + column + "`" + "=:" + column);
			}
			else
			{
				SqlBuilder.WHERE("`" + column + "`" + " IS :" + column);
			}
		}
		String updateQuery = SqlBuilder.SQL();
		logger.debug(updateQuery);
		return updateQuery;
	}

	public String getDeleteByCriteriaQuery(String tableName, Map<String, Object> deleteCriteriaColumnToValueMap)
	{
		SqlBuilder.BEGIN();
		SqlBuilder.DELETE_FROM(tableName);
		for (String column : deleteCriteriaColumnToValueMap.keySet())
		{
			if (deleteCriteriaColumnToValueMap.get(column) != null)
			{
				SqlBuilder.WHERE("`" + column + "`" + "=:" + column);
			}
			else
			{
				SqlBuilder.WHERE("`" + column + "`" + " IS :" + column);
			}
		}
		String updateQuery = SqlBuilder.SQL();
		logger.debug(updateQuery);
		return updateQuery;
	}

	public String getGetByCriteriaQuery(String tableName, Map<String, Object> criteriaColumnToValueMap)
	{
		SqlBuilder.BEGIN();
		SqlBuilder.SELECT("*");
		SqlBuilder.FROM(tableName);
		for (String column : criteriaColumnToValueMap.keySet())
		{
			if (criteriaColumnToValueMap.get(column) != null)
			{
				SqlBuilder.WHERE("`" + column + "`" + "=:" + column);
			}
			else
			{
				SqlBuilder.WHERE("`" + column + "`" + " IS :" + column);
			}
		}
		String getQuery = SqlBuilder.SQL();
		logger.debug(getQuery);
		return getQuery;
	}

	public String getGetByCriteriaQuery(String tableName, List<String> columnsInSelect,
	        Map<String, Object> criteriaColumnToValueMap)
	{
		SqlBuilder.BEGIN();
		for (String column : columnsInSelect)
		{
			SqlBuilder.SELECT(column);
		}
		SqlBuilder.FROM(tableName);
		for (String column : criteriaColumnToValueMap.keySet())
		{
			if (criteriaColumnToValueMap.get(column) != null)
			{
				SqlBuilder.WHERE("`" + column + "`" + "=:" + column);
			}
			else
			{
				SqlBuilder.WHERE("`" + column + "`" + " IS :" + column);
			}
		}
		String getQuery = SqlBuilder.SQL();
		logger.debug(getQuery);
		return getQuery;
	}
}
