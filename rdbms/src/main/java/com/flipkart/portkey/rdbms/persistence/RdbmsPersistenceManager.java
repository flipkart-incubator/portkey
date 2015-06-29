/**
 * 
 */
package com.flipkart.portkey.rdbms.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.ShardStatus;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.persistence.query.SqlQuery;
import com.flipkart.portkey.rdbms.persistence.config.RdbmsConnectionConfig;
import com.flipkart.portkey.rdbms.transaction.RdbmsTransactionManager;

/**
 * @author santosh.p
 */
public class RdbmsPersistenceManager
{
	private static final Logger logger = LoggerFactory.getLogger(RdbmsPersistenceManager.class);
	private final DataSource master;
	private final List<DataSource> slaves;
	private DataSourceTransactionManager transactionManager;
	private static StopWatch stopwatch = new Slf4JStopWatch(logger);

	public RdbmsPersistenceManager(RdbmsConnectionConfig config)
	{
		this.master = config.getMaster();
		if (config.getSlaves() == null)
		{
			this.slaves = new ArrayList<DataSource>();
		}
		else
		{
			this.slaves = config.getSlaves();
		}
		transactionManager = new DataSourceTransactionManager();
		transactionManager.setDataSource(master);
	}

	public ShardStatus healthCheck()
	{
		if (isAvailableForWrite())
			return ShardStatus.AVAILABLE_FOR_WRITE;
		else if (isAvailableForRead())
			return ShardStatus.AVAILABLE_FOR_READ;
		else
			return ShardStatus.UNAVAILABLE;
	}

	private boolean isAvailableForWrite()
	{
		try
		{
			new JdbcTemplate(master).execute("SELECT 1 FROM DUAL");
		}
		catch (DataAccessException e)
		{
			return false;
		}
		return true;
	}

	private boolean isAvailableForRead()
	{
		for (DataSource slave : slaves)
		{
			try
			{
				new JdbcTemplate(slave).execute("SELECT 1 FROM DUAL");
			}
			catch (DataAccessException e)
			{
				logger.info("Exception while trying to execute health check query on slave" + slave, e);
				continue;
			}
			return true;
		}
		return false;
	}

	public int executeUpdate(String query, Map<String, Object> parameterMap) throws QueryExecutionException
	{
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		try
		{
			return temp.update(query, parameterMap);
		}
		catch (DataAccessException e)
		{
			throw new QueryExecutionException("Exception while trying to execute update, query=" + query
			        + ", parameter map=" + parameterMap, e);
		}
	}

	@Transactional
	public int executeUpdates(List<SqlQuery> queryList, boolean failIfNoRowsAreUpdated) throws QueryExecutionException
	{
		TransactionDefinition def = new DefaultTransactionDefinition();
		TransactionStatus status = transactionManager.getTransaction(def);
		int totalRowsUpdated = 0;
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(master);
		for (SqlQuery query : queryList)
		{
			try
			{
				int rowsUpdated = temp.update(query.getQuery(), query.getColumnToValueMap());
				if (failIfNoRowsAreUpdated && rowsUpdated == 0)
				{
					transactionManager.rollback(status);
					throw new QueryExecutionException("No rows updated after executing query, rolling back, query= "
					        + query.getQuery());
				}
				totalRowsUpdated += rowsUpdated;
			}
			catch (DataAccessException e)
			{
				transactionManager.rollback(status);
				throw new QueryExecutionException(
				        "Exception while trying to execute atomic updates, failed while executing " + query.getQuery(),
				        e);
			}
		}
		transactionManager.commit(status);
		return totalRowsUpdated;
	}

	public int executeUpdates(List<SqlQuery> sqlQueryList) throws QueryExecutionException
	{
		return executeUpdates(sqlQueryList, false);
	}

	private <T extends Entity> List<T> executeQuery(DataSource dataSource, String query,
	        Map<String, Object> columnToValueMap, RowMapper<T> mapper) throws QueryExecutionException
	{
		List<T> result;
		NamedParameterJdbcTemplate temp = new NamedParameterJdbcTemplate(dataSource);
		try
		{
			result = temp.query(query, columnToValueMap, mapper);
		}
		catch (DataAccessException e)
		{
			throw new QueryExecutionException("Exception while trying to execute get query " + query + " on "
			        + dataSource, e);
		}
		return result;
	}

	public <T extends Entity> List<T> executeQuery(boolean readMaster, String query,
	        Map<String, Object> columnToValueMap, RowMapper<T> mapper) throws QueryExecutionException
	{
		if (readMaster)
		{
			return executeQuery(master, query, columnToValueMap, mapper);
		}
		else
		{
			for (DataSource slave : slaves)
			{
				try
				{
					return executeQuery(slave, query, columnToValueMap, mapper);
				}
				catch (QueryExecutionException e)
				{
					logger.warn("Exception while trying to execute get query " + query + " on slave " + slave, e);
					continue;
				}
			}
			return executeQuery(master, query, columnToValueMap, mapper);
		}
	}

	private List<Map<String, Object>> executeQuery(DataSource dataSource, String query,
	        Map<String, Object> columnToValueMap) throws QueryExecutionException
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

	public List<Map<String, Object>> executeQuery(String sql, Map<String, Object> criteria, boolean readMaster)
	        throws QueryExecutionException
	{
		if (readMaster)
		{
			return executeQuery(master, sql, criteria);
		}
		else
		{
			for (DataSource slave : slaves)
			{
				try
				{
					return executeQuery(slave, sql, criteria);
				}
				catch (QueryExecutionException e)
				{
					logger.warn("Exception while trying to execute get query " + sql + " on slave " + slave, e);
					continue;
				}
			}
			return executeQuery(master, sql, criteria);
		}
	}

	public RdbmsTransactionManager getTransactionManager()
	{
		return new RdbmsTransactionManager(master);
	}
}
