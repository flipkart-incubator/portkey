package com.flipkart.portkey.rdbms.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.flipkart.portkey.rdbms.metadata.annotation.JoinCriteria;
import com.flipkart.portkey.rdbms.metadata.annotation.JoinField;

public class RdbmsJoinMetaData extends RdbmsMetaData
{
	private List<String> tableList = new ArrayList<String>();
	private Map<String, JoinField> fieldNameToJoinFieldMap = new HashMap<String, JoinField>();
	private Map<String, String> tableToAliasMap = new HashMap<String, String>();
	private List<JoinCriteria> joinCriteriaList = new ArrayList<JoinCriteria>();
	private String joinQuery = null;
	private Map<String, String> fieldNameToTableNameMap = new HashMap<String, String>();

	public void setTableList(String[] tableList)
	{
		this.tableList = Arrays.asList(tableList);
	}

	public void addToTableList(String table)
	{
		tableList.add(table);
	}

	public List<String> getTableList()
	{
		return tableList;
	}

	public void setTableToAliasMap(Map<String, String> tableToAliasMap)
	{
		this.tableToAliasMap = tableToAliasMap;
	}

	public void addToTableToAliasMap(String table, String alias)
	{
		this.tableToAliasMap.put(table, alias);
	}

	public Map<String, String> getTableToAliasMap()
	{
		return tableToAliasMap;
	}

	public String getAliasFromTableName(String tableName)
	{
		return tableToAliasMap.get(tableName);
	}

	public String getJoinQuery()
	{
		return joinQuery;
	}

	public void setJoinQuery(String joinQuery)
	{
		this.joinQuery = joinQuery;
	}

	public void setFieldNameToRdbmsJoinFieldMap(Map<String, JoinField> fieldNameToRdbmsJoinFieldMap)
	{
		this.fieldNameToJoinFieldMap = fieldNameToRdbmsJoinFieldMap;
	}

	public void addJoinField(String fieldName, JoinField joinField)
	{
		fieldNameToJoinFieldMap.put(fieldName, joinField);
	}

	public Map<String, JoinField> getFieldNameToRdbmsJoinFieldMap()
	{
		return fieldNameToJoinFieldMap;
	}

	public JoinField getJoinFieldFromFieldName(String fieldName)
	{
		return fieldNameToJoinFieldMap.get(fieldName);
	}

	public void setJoinCriteriaList(List<JoinCriteria> joinCriteriaList)
	{
		this.joinCriteriaList = joinCriteriaList;
	}

	public void addToJoinCriteriaList(JoinCriteria joinCriteria)
	{
		this.joinCriteriaList.add(joinCriteria);
	}

	public List<JoinCriteria> getJoinCriteriaList()
	{
		return joinCriteriaList;
	}

	public void setFieldNameToTableNameMap(Map<String, String> fieldNameToTableNameMap)
	{
		this.fieldNameToTableNameMap = fieldNameToTableNameMap;
	}

	public void addToFieldNameToTableNameMap(String fieldName, String tableName)
	{
		this.fieldNameToTableNameMap.put(fieldName, tableName);
	}

	public Map<String, String> getFieldNameToTableNameMap()
	{
		return fieldNameToTableNameMap;
	}

	public String getTableNameFromFieldName(String fieldName)
	{
		return fieldNameToTableNameMap.get(fieldName);
	}

	// private String shardKey;
	// private List<String> primaryKeys = new ArrayList<String>();
	// private Map<String, RdbmsField> fieldNameToRdbmsFieldMap = new HashMap<String, RdbmsField>();
	// private Map<String, Serializer> fieldNameToSerializerMap = new HashMap<String, Serializer>();
	// private Map<String, String> fieldNameToRdbmsColumnMap = new HashMap<String, String>();
	// private Map<String, String> rdbmsColumnToFieldNameMap = new HashMap<String, String>();
	// private Map<String, Field> fieldNameToFieldMap = new HashMap<String, Field>();
}
