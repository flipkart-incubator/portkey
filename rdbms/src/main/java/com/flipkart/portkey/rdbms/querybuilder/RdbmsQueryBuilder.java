/**
 * 
 */
package com.flipkart.portkey.rdbms.querybuilder;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.jdbc.SqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flipkart.portkey.common.util.PortKeyUtils;
import com.flipkart.portkey.rdbms.metadata.RdbmsJoinMetaData;
import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;
import com.flipkart.portkey.rdbms.metadata.annotation.JoinCriteria;
import com.flipkart.portkey.rdbms.metadata.annotation.JoinField;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;

/**
 * @author santosh.p
 */
public class RdbmsQueryBuilder
{
	private static Logger logger = LoggerFactory.getLogger(RdbmsQueryBuilder.class);
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

	public String getUpdateByCriteriaQuery(String tableName, Map<String, Object> updateColumnToValueMap,
	        Map<String, Object> criteriaColumnToValueMap, Map<String, Object> placeHolderToValueMap)
	{
		SqlBuilder.BEGIN();
		SqlBuilder.UPDATE(tableName);
		for (String column : updateColumnToValueMap.keySet())
		{
			if (updateColumnToValueMap.get(column) != null
			        && updateColumnToValueMap.get(column).getClass().equals(RdbmsSpecialValue.class))
			{
				RdbmsSpecialValue specialValue = (RdbmsSpecialValue) updateColumnToValueMap.get(column);
				SqlBuilder.SET("`" + column + "`" + "=" + PortKeyUtils.toString(specialValue));
			}
			else
			{
				String placeHolder = column + "_update";
				SqlBuilder.SET("`" + column + "`" + "=:" + placeHolder);
				placeHolderToValueMap.put(placeHolder, updateColumnToValueMap.get(column));
			}
		}
		for (String column : criteriaColumnToValueMap.keySet())
		{
			String placeHolder = column + "_criteria";
			placeHolderToValueMap.put(placeHolder, criteriaColumnToValueMap.get(column));
			if (criteriaColumnToValueMap.get(column) != null)
			{
				SqlBuilder.WHERE("`" + column + "`" + "=:" + placeHolder);
			}
			else
			{
				SqlBuilder.WHERE("`" + column + "`" + " IS :" + placeHolder);
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

	public String getGetByJoinCriteriaQuery(RdbmsJoinMetaData joinMetaData, List<String> fieldNames,
	        List<String> criteriaAttributes)
	{
		List<JoinCriteria> joinCriteriaList = joinMetaData.getJoinCriteriaList();
		StringBuilder query = new StringBuilder("SELECT ");
		StringBuilder attributeListBuilder = new StringBuilder();
		for (String fieldName : fieldNames)
		{
			JoinField field = joinMetaData.getJoinFieldFromFieldName(fieldName);
			attributeListBuilder.append(joinMetaData.getAliasFromTableName(field.tableName()));
			attributeListBuilder.append(".");
			attributeListBuilder.append(field.columnName());
			attributeListBuilder.append(", ");
		}
		query.append(attributeListBuilder.substring(0, attributeListBuilder.length() - 2));
		query.append("\nFROM ");
		StringBuilder joinCriteriaBuilder = new StringBuilder();
		boolean firstCriteria = true;
		for (JoinCriteria criteria : joinCriteriaList)
		{
			if (firstCriteria)
			{
				joinCriteriaBuilder.append(criteria.srcTable());
				joinCriteriaBuilder.append(" AS ");
				joinCriteriaBuilder.append(joinMetaData.getAliasFromTableName(criteria.srcTable()));
				firstCriteria = false;
			}
			joinCriteriaBuilder.append(" ");
			joinCriteriaBuilder.append(criteria.joinType());
			joinCriteriaBuilder.append(" ");
			joinCriteriaBuilder.append(criteria.destTable());
			joinCriteriaBuilder.append(" AS ");
			joinCriteriaBuilder.append(joinMetaData.getAliasFromTableName(criteria.destTable()));
			joinCriteriaBuilder.append(" ON ");
			joinCriteriaBuilder.append(joinMetaData.getAliasFromTableName(criteria.srcTable()));
			joinCriteriaBuilder.append(".");
			joinCriteriaBuilder.append(criteria.srcColumn());
			joinCriteriaBuilder.append(criteria.criteriaValue());
			joinCriteriaBuilder.append(joinMetaData.getAliasFromTableName(criteria.destTable()));
			joinCriteriaBuilder.append(".");
			joinCriteriaBuilder.append(criteria.destColumn());
		}
		query.append(joinCriteriaBuilder);

		if (criteriaAttributes != null && criteriaAttributes.size() > 0)
		{
			StringBuilder queryCriteriaBuilder = new StringBuilder();
			queryCriteriaBuilder.append("\nWHERE ");
			for (String attribute : criteriaAttributes)
			{
				queryCriteriaBuilder.append(joinMetaData.getAliasFromTableName(joinMetaData
				        .getTableNameFromFieldName(attribute)) + "." + attribute + "=:" + attribute);
				queryCriteriaBuilder.append(",");
			}
			query.append(queryCriteriaBuilder.substring(0, queryCriteriaBuilder.length() - 1));
		}
		logger.debug(query.toString());
		return query.toString();
	}
}
