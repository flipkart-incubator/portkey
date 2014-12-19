/**
 * 
 */
package com.flipkart.portkey.rdbms.mapper;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.RowMapper;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;
import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;

/**
 * @author santosh.p
 */
public class RdbmsMapper<T> implements RowMapper<T>
{
	private static final Logger logger = Logger.getLogger(RdbmsMapper.class);
	private Class<Entity> clazz;

	protected RdbmsMapper(Class<T> clazz)
	{
		// TODO: remove this casting
		this.clazz = (Class<Entity>) clazz;
	}

	public static <T> RdbmsMapper<T> getInstance(Class<T> clazz)
	{
		return new RdbmsMapper<T>(clazz);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
	 */
	public T mapRow(ResultSet resultSet, int rowNum) throws SQLException
	{
		// make sure resultset is not null
		if (resultSet == null)
		{
			return null;
		}
		Entity bean;
		try
		{
			bean = clazz.newInstance();
		}
		catch (InstantiationException e)
		{
			logger.info("Exception in creating class instance:" + e);
			return null;
		}
		catch (IllegalAccessException e)
		{
			logger.info("Exception in creating class instance:" + e);
			return null;
		}

		RdbmsTableMetaData tableMetaData = RdbmsMetaDataCache.getMetaData(clazz);
		ResultSetMetaData metadata = resultSet.getMetaData();

		for (int i = 1; i <= metadata.getColumnCount(); i++)
		{
			String columnName = metadata.getColumnLabel(i);
			String fieldName = tableMetaData.getRdbmsColumnToFieldNameMap().get(columnName);

			if ((fieldName == null || fieldName.isEmpty()))
			{
				fieldName = columnName;
			}

			Object columnValue = resultSet.getObject(i);
			Field field = tableMetaData.getFieldNameToFieldMap().get(fieldName);
			try
			{
				field.set(bean, columnValue);
			}
			catch (IllegalArgumentException e)
			{
				logger.info(e);
			}
			catch (IllegalAccessException e)
			{
				logger.info(e);
			}
		}
		return (T) bean;
	}
}
