package com.flipkart.portkey.rdbms.transaction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.persistence.TransactionManager;
import com.flipkart.portkey.rdbms.mapper.RdbmsMapper;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;
import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;
import com.flipkart.portkey.rdbms.persistence.RdbmsHelper;
import com.flipkart.portkey.rdbms.querybuilder.RdbmsQueryBuilder;

public class RdbmsTransactionManager implements TransactionManager
{
	private DataSource dataSource;
	private TransactionDefinition def;
	private TransactionStatus status;
	private DataSourceTransactionManager transactionManager;
	private RdbmsMetaDataCache metaDataCache = RdbmsMetaDataCache.getInstance();

	private boolean isTxnStarted;
	private boolean isTxnComitted;

	public RdbmsTransactionManager(DataSource dataSource)
	{
		this.dataSource = dataSource;
		isTxnStarted = false;
		isTxnComitted = false;
	}

	public void begin()
	{
		transactionManager = new DataSourceTransactionManager();
		transactionManager.setDataSource(dataSource);
		def = new DefaultTransactionDefinition();
		status = transactionManager.getTransaction(def);
		isTxnStarted = true;
	}

	private void checkIfTxnIsActive() throws QueryExecutionException
	{
		if (isTxnStarted == false)
		{
			throw new QueryExecutionException(
			        "Transaction is not active, call begin method before starting to execute updates");
		}
		if (isTxnComitted == true)
		{
			throw new QueryExecutionException(
			        "Transaction is already comitted, start new transaction by calling begin method");
		}
	}

	public int executeUpdate(String query, Map<String, Object> columnToValueMap) throws QueryExecutionException
	{
		checkIfTxnIsActive();

		int rowsUpdated = 0;

		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(dataSource);
		try
		{
			rowsUpdated = temp.update(query, columnToValueMap);
		}
		catch (DataAccessException e)
		{
			throw new QueryExecutionException("Exception while trying to execute update:" + query + ", "
			        + columnToValueMap, e);
		}
		return rowsUpdated;
	}

	public <T extends Entity> List<T> executeQuery(String query, Map<String, Object> columnToValueMap,
	        RdbmsMapper<T> mapper) throws QueryExecutionException
	{
		checkIfTxnIsActive();

		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(dataSource);
		try
		{
			return temp.query(query, columnToValueMap, mapper);
		}
		catch (DataAccessException e)
		{
			throw new QueryExecutionException("Exception while trying to execute query:" + query + ", "
			        + columnToValueMap, e);
		}
	}

	private List<Map<String, Object>> executeQuery(String query, Map<String, Object> columnToValueMap)
	        throws QueryExecutionException
	{
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(dataSource);
		try
		{
			return temp.queryForList(query, columnToValueMap);
		}
		catch (DataAccessException e)
		{
			throw new QueryExecutionException("Exception while trying to execute get query " + query + " on "
			        + dataSource, e);
		}
	}

	public void rollback()
	{
		transactionManager.rollback(status);
	}

	public void commit()
	{
		transactionManager.commit(status);
	}

	public <T extends Entity> int insert(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(bean.getClass());
		String insertQuery = RdbmsQueryBuilder.getInstance().getInsertQuery(metaData);
		Map<String, Object> columnToValueMap = RdbmsHelper.generateColumnToValueMap(bean, metaData);
		return executeUpdate(insertQuery, columnToValueMap);
	}

	public <T extends Entity> int upsert(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(bean.getClass());
		String upsertQuery = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData);
		Map<String, Object> columnToValueMap = RdbmsHelper.generateColumnToValueMap(bean, metaData);
		return executeUpdate(upsertQuery, columnToValueMap);
	}

	public <T extends Entity> int upsert(T bean, List<String> columnsToBeUpdatedOnDuplicate)
	        throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(bean.getClass());
		String upsertQuery = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData, columnsToBeUpdatedOnDuplicate);
		Map<String, Object> columnToValueMap = RdbmsHelper.generateColumnToValueMap(bean, metaData);
		return executeUpdate(upsertQuery, columnToValueMap);
	}

	public <T extends Entity> int update(T bean) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(bean.getClass());
		String updateQuery = RdbmsQueryBuilder.getInstance().getUpdateByPkQuery(metaData);
		Map<String, Object> columnToValueMap = RdbmsHelper.generateColumnToValueMap(bean, metaData);
		return executeUpdate(updateQuery, columnToValueMap);
	}

	public <T extends Entity> int update(Class<T> clazz, Map<String, Object> updateValuesMap,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(clazz);
		Map<String, Object> updateColumnToValueMap = RdbmsHelper.generateColumnToValueMap(clazz, updateValuesMap);
		Map<String, Object> criteriaColumnToValueMap = RdbmsHelper.generateColumnToValueMap(clazz, criteria);
		String tableName = metaData.getTableName();
		Map<String, Object> placeHolderToValueMap = new HashMap<String, Object>();
		String updateQuery =
		        RdbmsQueryBuilder.getInstance().getUpdateByCriteriaQuery(tableName, updateColumnToValueMap,
		                criteriaColumnToValueMap, placeHolderToValueMap);
		return executeUpdate(updateQuery, placeHolderToValueMap);
	}

	public <T extends Entity> int delete(Class<T> clazz, Map<String, Object> criteria) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(clazz);
		String tableName = metaData.getTableName();
		Map<String, Object> deleteCriteriaColumnToValueMap = RdbmsHelper.generateColumnToValueMap(clazz, criteria);
		String deleteQuery =
		        RdbmsQueryBuilder.getInstance().getDeleteByCriteriaQuery(tableName, deleteCriteriaColumnToValueMap);
		return executeUpdate(deleteQuery, deleteCriteriaColumnToValueMap);
	}

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = metaDataCache.getTableMetaData(clazz);
		String tableName = metaData.getTableName();
		Map<String, Object> criteriaColumnToValueMap = RdbmsHelper.generateColumnToValueMap(clazz, criteria);
		String getQuery = RdbmsQueryBuilder.getInstance().getGetByCriteriaQuery(tableName, criteriaColumnToValueMap);
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		return executeQuery(getQuery, criteriaColumnToValueMap, mapper);
	}

	public <T extends Entity> List<T> getByCriteria(Class<T> clazz, List<String> attributeNames,
	        Map<String, Object> criteria) throws QueryExecutionException
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getTableMetaData(clazz);
		String tableName = metaData.getTableName();
		List<String> columnsInSelect = RdbmsHelper.generateColumnsListFromFieldNamesList(metaData, attributeNames);
		Map<String, Object> criteriaColumnToValueMap = RdbmsHelper.generateColumnToValueMap(clazz, criteria);
		String getQuery =
		        RdbmsQueryBuilder.getInstance().getGetByCriteriaQuery(tableName, columnsInSelect,
		                criteriaColumnToValueMap);
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		return executeQuery(getQuery, criteriaColumnToValueMap, mapper);
	}

	public <T extends Entity> List<T> getBySql(Class<T> clazz, String sql, Map<String, Object> criteria)
	        throws QueryExecutionException
	{
		RdbmsMapper<T> mapper = RdbmsMapper.getInstance(clazz);
		return executeQuery(sql, criteria, mapper);
	}

	public List<Map<String, Object>> getBySql(String sql, Map<String, Object> criteria) throws QueryExecutionException
	{
		return executeQuery(sql, criteria);
	}

	public int updateBySql(String sql, Map<String, Object> criteria) throws QueryExecutionException
	{
		return executeUpdate(sql, criteria);
	}
}
